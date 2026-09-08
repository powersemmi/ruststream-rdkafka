//! Per-delivery and per-batch context fields exposed to handlers.
//!
//! [`KafkaContext`] carries the Kafka delivery metadata that is not part of the payload or the
//! headers, plus the subscription's reposition handle. Request it in a handler by typing the
//! context parameter as `Context<'_, KafkaContext>` and read individual fields with the
//! zero-sized keys in [`keys`], or bind one field as a parameter with the core's `Ctx<K>`
//! extractor.
//!
//! A batch handler gets [`KafkaBatchContext`] instead: a batch spans many deliveries, so it
//! carries only what the whole subscription shares - the reposition handle, under the same
//! [`keys::SeekHandle`] key. Per-delivery coordinates ride the batch's elements instead (a
//! `&[Message<H, T>]` batch reads them off each element's typed header contract), and the two
//! being separate types is what keeps a batch body from naming a position that belongs to one
//! record.

use std::sync::Arc;

use bytes::Bytes;
use ruststream::{BuildBatchContext, BuildContext, Field};

use crate::message::KafkaMessage;
use crate::seek::{KafkaPosition, KafkaSeeker};

/// Native Kafka delivery metadata plus this subscription's reposition handle, built once per
/// delivery.
#[derive(Debug, Clone)]
pub struct KafkaContext {
    topic: String,
    partition: i32,
    offset: i64,
    timestamp_millis: Option<i64>,
    key: Option<Bytes>,
    seeker: Arc<KafkaSeeker>,
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

    /// This delivery's own coordinates: seeking to them redelivers exactly this record, and the
    /// ordered suffix behind it on the partition.
    #[must_use]
    pub fn position(&self) -> KafkaPosition {
        KafkaPosition::topic_offset(&self.topic, self.partition, self.offset)
    }

    /// The handle repositioning the subscription this delivery came from.
    #[must_use]
    pub fn seeker(&self) -> &KafkaSeeker {
        &self.seeker
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
            // The subscription minted the handle when it opened, so carrying it costs one
            // reference-count bump per delivery, not a producer or consumer setup.
            seeker: msg.seeker_handle(),
        }
    }
}

/// The in-process transport carries the same context, so a service reading its broker fields
/// mounts on [`KafkaTestBroker`](crate::testing::KafkaTestBroker) unchanged and is testable with
/// `TestApp`. What it reports is what that transport actually has: the topic and the log offset
/// are real and the seeker really repositions, the partition is zero (the transport gives each
/// topic one), and there is no record timestamp because it stamps none.
#[cfg(feature = "testing")]
impl BuildContext<crate::testing::KafkaTestMessage> for KafkaContext {
    fn build(msg: &crate::testing::KafkaTestMessage) -> Self {
        Self {
            topic: msg.topic().to_owned(),
            partition: 0,
            offset: msg.offset(),
            timestamp_millis: None,
            key: msg.key().map(Bytes::copy_from_slice),
            seeker: msg.seeker_handle(),
        }
    }
}

/// The subscription-scoped context of a batch handler: the reposition handle every delivery of
/// the batch shares.
///
/// Built once per dispatched batch, off its first delivery. A batch has no single position - it
/// spans many records - so the coordinates a body reacts to travel with the elements, and
/// keeping this a type of its own is what rejects a batch body naming [`KafkaContext`] at compile
/// time.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream::prelude::*;
/// use ruststream_rdkafka::context::{KafkaBatchContext, keys::SeekHandle};
/// use ruststream_rdkafka::{KafkaPosition, prelude::Seeker as _};
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64, resume_at: Option<i64>, partition: i32 }
///
/// struct Reprocess;
///
/// impl Handle<[Order], (), (), KafkaBatchContext> for Reprocess {
///     async fn handle(
///         &self,
///         orders: &[Order],
///         _outs: &(),
///         ctx: &mut Context<'_, KafkaBatchContext>,
///     ) -> Result<(), Vec<HandlerOutcome>> {
///         // The batch is settled first; the reposition then opens the next batch at the
///         // target the producer marked on one of the elements.
///         let target = orders
///             .iter()
///             .find_map(|order| order.resume_at.map(|at| (order.partition, at)));
///         if let Some((partition, at)) = target
///             && ctx
///                 .context(SeekHandle)
///                 .seek(KafkaPosition::offset(partition, at))
///                 .await
///                 .is_err()
///         {
///             return Err(orders.iter().map(|_| HandlerOutcome::retry()).collect());
///         }
///         Ok(())
///     }
/// }
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct KafkaBatchContext {
    seeker: Arc<KafkaSeeker>,
}

impl KafkaBatchContext {
    /// The handle repositioning the subscription this batch came from.
    #[must_use]
    pub fn seeker(&self) -> &KafkaSeeker {
        &self.seeker
    }
}

impl BuildBatchContext<KafkaMessage> for KafkaBatchContext {
    fn build(first: &KafkaMessage) -> Self {
        Self {
            seeker: first.seeker_handle(),
        }
    }
}

/// The in-process transport carries the batch context too: its seeker repositions the retained
/// log, so a batch body that replays is testable with `TestApp`.
#[cfg(feature = "testing")]
impl BuildBatchContext<crate::testing::KafkaTestMessage> for KafkaBatchContext {
    fn build(first: &crate::testing::KafkaTestMessage) -> Self {
        Self {
            seeker: first.seeker_handle(),
        }
    }
}

/// Zero-sized [`Field`] keys reading one [`KafkaContext`] field each.
pub mod keys {
    use ruststream::ContextField;

    use super::{Field, KafkaBatchContext, KafkaContext, KafkaPosition, KafkaSeeker};

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

    /// Reads this delivery's own log coordinates, in the form
    /// [`Seeker::seek`](ruststream::Seeker::seek) takes: seeking to them redelivers exactly this
    /// record.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_rdkafka::prelude::*;
    /// # #[derive(serde::Deserialize)]
    /// # struct Order { id: u64 }
    ///
    /// /// Rewinds to the record it is holding when a downstream dependency is unavailable, so
    /// /// the partition resumes here rather than from the group's committed position.
    /// #[subscriber("orders")]
    /// async fn place(
    ///     order: &Order,
    ///     Ctx(here): Ctx<Position>,
    ///     Ctx(seeker): Ctx<SeekHandle>,
    /// ) -> HandlerOutcome {
    ///     if order.id == 0 && seeker.seek(here).await.is_err() {
    ///         return HandlerOutcome::retry();
    ///     }
    ///     HandlerOutcome::ack()
    /// }
    /// ```
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct Position;

    impl Field<KafkaContext> for Position {
        type Value<'a> = KafkaPosition;

        fn get(self, src: &KafkaContext) -> KafkaPosition {
            src.position()
        }
    }

    impl ContextField for Position {
        type Context = KafkaContext;
        type Value = KafkaPosition;
        fn read(self, src: &KafkaContext) -> KafkaPosition {
            src.position()
        }
    }

    /// Reads the subscription's reposition handle.
    ///
    /// The same key serves the per-delivery [`KafkaContext`] and the batch-scoped
    /// [`KafkaBatchContext`], because a seek is a subscription operation either way.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_rdkafka::prelude::*;
    /// # #[derive(serde::Deserialize)]
    /// # struct Job { id: u64, resume_at: Option<i64> }
    ///
    /// /// Jumps the partition past a run the producer marked unprocessable.
    /// #[subscriber("jobs")]
    /// async fn work(
    ///     job: &Job,
    ///     Ctx(partition): Ctx<Partition>,
    ///     Ctx(seeker): Ctx<SeekHandle>,
    /// ) -> HandlerOutcome {
    ///     if let Some(resume_at) = job.resume_at
    ///         && seeker
    ///             .seek(KafkaPosition::offset(partition, resume_at))
    ///             .await
    ///             .is_err()
    ///     {
    ///         return HandlerOutcome::retry();
    ///     }
    ///     HandlerOutcome::ack()
    /// }
    /// ```
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct SeekHandle;

    impl Field<KafkaContext> for SeekHandle {
        type Value<'a> = &'a KafkaSeeker;

        fn get(self, src: &KafkaContext) -> &KafkaSeeker {
            src.seeker()
        }
    }

    impl ContextField for SeekHandle {
        type Context = KafkaContext;
        type Value = KafkaSeeker;
        fn read(self, src: &KafkaContext) -> KafkaSeeker {
            src.seeker().clone()
        }
    }

    impl Field<KafkaBatchContext> for SeekHandle {
        type Value<'a> = &'a KafkaSeeker;

        fn get(self, src: &KafkaBatchContext) -> &KafkaSeeker {
            src.seeker()
        }
    }
}
