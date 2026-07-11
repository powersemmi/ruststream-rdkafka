//! Per-delivery context fields exposed to handlers.
//!
//! [`KafkaContext`] carries the Kafka delivery metadata that is not part of the payload or the
//! headers. Request it in a handler by typing the context parameter as
//! `Context<'_, KafkaContext>` and read individual fields with the zero-sized keys in [`keys`].

use bytes::Bytes;
use ruststream::{BuildContext, Field};

use crate::message::KafkaMessage;

/// Native Kafka delivery metadata, built once per delivery.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KafkaContext {
    topic: String,
    partition: i32,
    offset: i64,
    timestamp_millis: Option<i64>,
    key: Option<Bytes>,
}

impl KafkaContext {
    /// The topic the record was consumed from.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// The partition the record was consumed from.
    #[must_use]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// The record's offset within its partition.
    #[must_use]
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// The record's timestamp in milliseconds since the epoch, when the broker provided one.
    #[must_use]
    pub fn timestamp_millis(&self) -> Option<i64> {
        self.timestamp_millis
    }

    /// The record key, or `None` for keyless records.
    #[must_use]
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }
}

impl BuildContext<KafkaMessage> for KafkaContext {
    fn build(msg: &KafkaMessage) -> Self {
        Self {
            topic: msg.topic().to_owned(),
            partition: msg.partition(),
            offset: msg.offset(),
            timestamp_millis: msg.timestamp_millis(),
            key: msg.key().map(Bytes::copy_from_slice),
        }
    }
}

/// Zero-sized [`Field`] keys reading one [`KafkaContext`] field each.
pub mod keys {
    use ruststream::ContextField;

    use super::{Field, KafkaContext};

    use crate::eos::SourceOffset;

    /// Reads the source topic name.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct Topic;

    impl Field<KafkaContext> for Topic {
        type Value<'a> = &'a str;

        fn get(self, src: &KafkaContext) -> &str {
            src.topic()
        }
    }

    impl ContextField for Topic {
        type Context = KafkaContext;
        type Value = String;
        fn read(self, src: &KafkaContext) -> String {
            src.topic().to_owned()
        }
    }

    /// Reads the source partition.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct Partition;

    impl Field<KafkaContext> for Partition {
        type Value<'a> = i32;

        fn get(self, src: &KafkaContext) -> i32 {
            src.partition()
        }
    }

    impl ContextField for Partition {
        type Context = KafkaContext;
        type Value = i32;
        fn read(self, src: &KafkaContext) -> i32 {
            src.partition()
        }
    }

    /// Reads the record offset.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct Offset;

    impl Field<KafkaContext> for Offset {
        type Value<'a> = i64;

        fn get(self, src: &KafkaContext) -> i64 {
            src.offset()
        }
    }

    impl ContextField for Offset {
        type Context = KafkaContext;
        type Value = i64;
        fn read(self, src: &KafkaContext) -> i64 {
            src.offset()
        }
    }

    /// Reads the record timestamp in milliseconds since the epoch, when present.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct TimestampMillis;

    impl Field<KafkaContext> for TimestampMillis {
        type Value<'a> = Option<i64>;

        fn get(self, src: &KafkaContext) -> Option<i64> {
            src.timestamp_millis()
        }
    }

    impl ContextField for TimestampMillis {
        type Context = KafkaContext;
        type Value = Option<i64>;
        fn read(self, src: &KafkaContext) -> Option<i64> {
            src.timestamp_millis()
        }
    }

    /// Reads the record key, when present.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct Key;

    impl Field<KafkaContext> for Key {
        type Value<'a> = Option<&'a [u8]>;

        fn get(self, src: &KafkaContext) -> Option<&[u8]> {
            src.key()
        }
    }

    impl ContextField for Key {
        type Context = KafkaContext;
        type Value = Option<Vec<u8>>;
        fn read(self, src: &KafkaContext) -> Option<Vec<u8>> {
            src.key().map(<[u8]>::to_vec)
        }
    }

    /// Reads the delivery's source coordinates as one value, the form
    /// [`EosPipeline::publish`](crate::EosPipeline::publish) takes.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct Source;

    impl Field<KafkaContext> for Source {
        type Value<'a> = SourceOffset;

        fn get(self, src: &KafkaContext) -> SourceOffset {
            SourceOffset::new(src.topic(), src.partition(), src.offset())
        }
    }

    impl ContextField for Source {
        type Context = KafkaContext;
        type Value = SourceOffset;
        fn read(self, src: &KafkaContext) -> SourceOffset {
            SourceOffset::new(src.topic(), src.partition(), src.offset())
        }
    }
}
