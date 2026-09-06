//! The Confluent wire-format envelope as byte-lane types.
//!
//! A registry-backed payload is `0x00`, a big-endian 4-byte schema id, then the datum (for
//! Protobuf, message-indexes and then the message). That is self-describing on the wire, so it
//! belongs on the core's byte lanes rather than behind a transcoding layer: [`IncomingFrame`]
//! arrives through [`Deserialized`] and [`OutgoingFrame`] leaves through [`Serialized`], with no
//! codec resolved for either.
//!
//! Splitting the envelope out of the value is what makes the lanes reachable at all. Resolving a
//! schema id is a registry conversation and therefore `async`; [`Deserialized::from_payload`] is
//! a sync associated function with no context to reach a registry from. So the envelope, which
//! needs nothing but the bytes, rides the lane, and the resolution stays where `async` is
//! allowed: one `await` in the handler on
//! [`avro::decode_framed`](crate::avro::decode_framed), and, on the publish side, one resolution
//! at startup ([`avro::Subject`](crate::avro::Subject)). Neither half needs a process-wide
//! registry singleton, and neither hides an I/O stall inside a decode.
//!
//! These two types are correct on their own: they carry no schema knowledge, so a service that
//! resolves schemas some other way (a pinned id, an out-of-band catalogue) uses them unchanged.

use std::convert::Infallible;

use ruststream::runtime::{
    Deserialized, Input, MessageWire, ReplyShape, Serialized, SerializedReply, SerializedWire,
    SoloDeserialized,
};
use ruststream::{BytesMut, CallerName, MessageHeaders, NoHeaders, OutgoingDestination};

use crate::error::KafkaError;
use crate::schema_registry::{WIRE_MAGIC, parse_envelope};

/// One delivery's Confluent envelope: the schema id it was written with, and the datum after it.
///
/// As a handler input (`&IncomingFrame<'_>`) the delivery's bytes reach the body exactly as they
/// arrived - the view borrows the broker's buffer, nothing is copied and no codec runs. The
/// payload is turned into a value by the format's own reader, which for a registry-backed topic
/// means resolving the writer schema first:
/// [`avro::decode_framed`](crate::avro::decode_framed) does both, and
/// [`protobuf::decode_framed`](crate::protobuf::decode_framed) needs no registry at all.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::Deserialized;
/// use ruststream_rdkafka::IncomingFrame;
///
/// # fn check() -> Result<(), Box<dyn std::error::Error>> {
/// // The wire form: the magic byte, the id, then the datum.
/// let payload = [0x00, 0x00, 0x00, 0x00, 0x2a, 0x08, 0x61];
/// let frame = IncomingFrame::from_payload(&payload)?;
/// assert_eq!(frame.schema_id(), 42);
/// assert_eq!(frame.datum(), &[0x08, 0x61]);
/// # Ok(())
/// # }
/// # check().unwrap();
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IncomingFrame<'a> {
    schema_id: u32,
    datum: &'a [u8],
}

impl<'a> IncomingFrame<'a> {
    /// The registry-assigned schema id the payload was written with.
    #[must_use]
    pub fn schema_id(&self) -> u32 {
        self.schema_id
    }

    /// The bytes after the envelope: an Avro datum, or a Protobuf message behind its
    /// message-indexes.
    #[must_use]
    pub fn datum(&self) -> &'a [u8] {
        self.datum
    }
}

impl Deserialized for IncomingFrame<'_> {
    type Output<'a> = IncomingFrame<'a>;
    type Error = KafkaError;

    fn from_payload(payload: &[u8]) -> Result<IncomingFrame<'_>, Self::Error> {
        let (schema_id, datum) = parse_envelope(payload).ok_or_else(|| {
            KafkaError::malformed(format!(
                "the delivery does not carry the Confluent wire format (a zero magic byte and a \
                 4-byte schema id), so it was not written by a registry-backed producer; its \
                 first bytes are {:02x?}",
                &payload[..payload.len().min(8)],
            ))
        })?;
        Ok(IncomingFrame { schema_id, datum })
    }
}

impl Input for IncomingFrame<'_> {
    type Axis = SoloDeserialized<IncomingFrame<'static>>;
}

/// A payload to publish under the Confluent envelope: the schema id the datum was written with,
/// and the datum.
///
/// The two halves stay apart until the publish path asks for the bytes, so the envelope is
/// written once, straight into the buffer that path already carries. Mint one from a resolved
/// subject ([`avro::Subject::frame`](crate::avro::Subject::frame)) rather than by hand, so the
/// id and the datum cannot disagree about which schema wrote it.
///
/// # Examples
///
/// ```
/// use ruststream::BytesMut;
/// use ruststream::runtime::Serialized;
/// use ruststream_rdkafka::OutgoingFrame;
///
/// # fn check() -> Result<(), Box<dyn std::error::Error>> {
/// let frame = OutgoingFrame::new(42, vec![0x08, 0x61]);
/// let mut buf = BytesMut::new();
/// assert_eq!(frame.wire_bytes(&mut buf)?, &[0x00, 0x00, 0x00, 0x00, 0x2a, 0x08, 0x61]);
/// # Ok(())
/// # }
/// # check().unwrap();
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OutgoingFrame {
    schema_id: u32,
    datum: Vec<u8>,
}

impl OutgoingFrame {
    /// Pairs a datum with the id of the schema it was written with.
    ///
    /// The pairing is the whole correctness of the wire format: a consumer decodes with the
    /// schema the id names, so a datum framed with any other id is unreadable. Prefer the
    /// per-format constructors, which take the id from the same schema they encode with.
    #[must_use]
    pub fn new(schema_id: u32, datum: Vec<u8>) -> Self {
        Self { schema_id, datum }
    }

    /// The registry-assigned schema id the envelope carries.
    #[must_use]
    pub fn schema_id(&self) -> u32 {
        self.schema_id
    }

    /// The encoded datum, without the envelope.
    #[must_use]
    pub fn datum(&self) -> &[u8] {
        &self.datum
    }
}

impl Serialized for OutgoingFrame {
    type Error = Infallible;

    fn wire_bytes<'a>(&'a self, buf: &'a mut BytesMut) -> Result<&'a [u8], Infallible> {
        // Into the publish path's own buffer: the envelope is the only thing this type still
        // owes the wire, and writing it here is what keeps the datum from being copied twice.
        buf.reserve(1 + 4 + self.datum.len());
        buf.extend_from_slice(&[WIRE_MAGIC]);
        buf.extend_from_slice(&self.schema_id.to_be_bytes());
        buf.extend_from_slice(&self.datum);
        Ok(&buf[..])
    }
}

impl MessageWire for OutgoingFrame {
    type Wire = SerializedWire;
}

impl ReplyShape for OutgoingFrame {
    type Body = Self;
    type Headers = ();
    type Wire = SerializedReply;
}

// A frame carries a schema, not a destination: the same message type is published to whatever
// topic the call site names, so the address is the caller's and the type declares none.
impl OutgoingDestination for OutgoingFrame {
    type Form = CallerName;
}

impl MessageHeaders for OutgoingFrame {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_framed_payload_lends_its_parts() {
        let payload = [0x00, 0x00, 0x00, 0x01, 0xff, 0x02, 0x04];
        let frame = IncomingFrame::from_payload(&payload).expect("framed");

        assert_eq!(frame.schema_id(), 511);
        assert_eq!(frame.datum(), &[0x02, 0x04]);
    }

    #[test]
    fn an_unframed_payload_names_what_it_is_missing() {
        let err = IncomingFrame::from_payload(br#"{"id":7}"#).expect_err("not framed");

        assert!(matches!(err, KafkaError::WireFormat(_)));
        assert!(err.to_string().contains("Confluent wire format"));
    }

    #[test]
    fn the_two_frames_roundtrip_through_the_wire() {
        let out = OutgoingFrame::new(1234, vec![0x08, 0x61, 0x62]);
        let mut buf = BytesMut::new();
        let wire = out.wire_bytes(&mut buf).expect("infallible");

        let back = IncomingFrame::from_payload(wire).expect("framed");
        assert_eq!(back.schema_id(), out.schema_id());
        assert_eq!(back.datum(), out.datum());
    }
}
