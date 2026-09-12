//! The in-process publisher.

use std::borrow::Cow;
use std::future::{Future, ready};
use std::sync::Arc;

use bytes::Bytes;
use ruststream::{DefaultPublish, OutgoingMessage, PairError, PublishPolicy, Publisher};

use super::broker::{ConnectedKafkaTestBroker, TestBrokerState};
use crate::error::KafkaError;
use crate::message::PARTITION_HEADER;
use crate::publisher::{KafkaOptions, KafkaPublish};

/// Publisher into the in-process router.
///
/// Mirrors [`KafkaPublisher`](crate::KafkaPublisher) delivery semantics minus the cluster: the
/// message name is the topic, and the partition-key header rides along for keyed worker lanes.
#[derive(Debug, Clone)]
pub struct KafkaTestPublisher {
    state: Arc<TestBrokerState>,
}

impl KafkaTestPublisher {
    pub(crate) fn new(state: Arc<TestBrokerState>) -> Self {
        Self { state }
    }
}

/// The in-process broker pairs the real [`KafkaPublish`] policy, so an application's include
/// sites and routers compile unchanged against either broker.
impl PublishPolicy<ConnectedKafkaTestBroker> for KafkaPublish {
    type Live = KafkaTestPublisher;

    fn pair(
        self,
        connected: &ConnectedKafkaTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher(self)))
    }
}

impl DefaultPublish for ConnectedKafkaTestBroker {
    type Policy = KafkaPublish;
}

/// And it pairs the framing policy too, so a mount site naming
/// [`KafkaPublish::framed`](crate::KafkaPublish::framed) is testable in process: the envelope a
/// reply carries on the in-process topic is the one a real broker would have put there.
#[cfg(feature = "protobuf")]
impl PublishPolicy<ConnectedKafkaTestBroker> for crate::protobuf::KafkaFramedPublish {
    type Live = crate::protobuf::KafkaFramedPublisher<KafkaTestPublisher>;

    fn pair(
        self,
        connected: &ConnectedKafkaTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        let (publish, framing) = self.into_parts();
        ready(Ok(crate::protobuf::KafkaFramedPublisher::new(
            connected.publisher(publish),
            framing,
        )))
    }
}

impl Publisher for KafkaTestPublisher {
    type Error = KafkaError;
    type Options = KafkaOptions;

    /// Routes `msg` to subscribers of the topic named by `msg.name()`.
    ///
    /// A partition named per record is recorded rather than honoured: the transport gives every
    /// topic one partition, so it carries the number on
    /// [`PARTITION_HEADER`](crate::PARTITION_HEADER) where the broker log can read it back, and
    /// the slot view answers `with_options` from what the call actually carried.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::InvalidOptions`] when the topic name is empty, and
    /// [`KafkaError::Closed`] once the transport this handle aliases has been shut down.
    fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        options: Option<&Self::Options>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        if msg.name().is_empty() {
            return ready(Err(KafkaError::InvalidOptions(
                "topic name must not be empty; the outgoing message name is the destination \
                 topic"
                    .to_owned(),
            )));
        }
        if let Err(err) = self.state.ensure_open(msg.name()) {
            return ready(Err(err));
        }
        let partition = options.and_then(|options| options.partition_setting());
        let headers = partition.map_or_else(
            || Cow::Borrowed(msg.headers()),
            |partition| {
                let mut headers = msg.headers().clone();
                headers.insert(PARTITION_HEADER, partition.to_string());
                Cow::Owned(headers)
            },
        );
        self.state.router.publish(
            msg.name(),
            &Bytes::copy_from_slice(msg.payload()),
            &headers,
            self.state.coordinator().as_ref(),
        );
        ready(Ok(()))
    }
}
