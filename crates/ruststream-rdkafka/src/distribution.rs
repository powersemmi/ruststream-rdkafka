//! Producer-side distribution policies built on the per-record partition setting.
//!
//! librdkafka's partitioner families (random, consistent, murmur2, fnv1a) cannot express
//! per-message round-robin, and keyless distribution may batch-stick to one partition. For
//! workloads with long, near-constant per-message processing times that unevenness turns into
//! one hot consumer and idle peers; [`RoundRobin`] pins each outgoing record to the next
//! partition in the cycle instead.

use std::sync::atomic::{AtomicU64, Ordering};

use ruststream::runtime::{ContextKind, Outgoing, PublishTransform, Reads};

use crate::message::PARTITION_KEY_HEADER;
use crate::publisher::KafkaOptions;

/// A [`PublishTransform`] distributing records round-robin across the first `count` partitions.
///
/// Each record is pinned to an incrementing counter modulo `count`, so the publisher targets
/// partitions 0..count in a cycle, one message each - the evenest possible spread for long,
/// near-constant-cost messages. A record that already names a partition or carries a record key
/// is left alone: keys exist for ordering, and overriding either would silently break the
/// caller's placement.
///
/// The cycle writes the same per-record setting the builder's
/// [`partition`](crate::KafkaPublishSteps::partition) step writes, so it mounts only over a
/// publisher whose [`Publisher::Options`](ruststream::Publisher::Options) are [`KafkaOptions`].
/// On an `Out` slot that setting arrives carrying whatever the call site chose, and the cycle
/// steps aside for it; a reply has no call site, so there the cycle always places.
///
/// The count is explicit on purpose (cheap and predictable); it must match the destination
/// topic's partition count, or the tail partitions simply receive nothing (a smaller count)
/// or publishes fail (a larger one).
///
/// # Examples
///
/// The transform is a step of the mount site's chain, right after the publish policy:
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream_rdkafka::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64 }
/// # #[derive(serde::Serialize, Outgoing)]
/// # #[outgoing(name = "work-items")]
/// # struct WorkItem { order_id: u64 }
///
/// #[ruststream::subscriber("orders", publish)]
/// async fn plan(order: &Order) -> WorkItem {
///     WorkItem { order_id: order.id }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("planner", "0.1.0"))
///         .with_broker(KafkaBroker::new(["localhost:9092"]), |b| {
///             b.include(plan)
///                 .out_reply(Publish::default())
///                 .transform(RoundRobin::partitions(8));
///         })
/// }
/// # }
/// ```
#[derive(Debug)]
pub struct RoundRobin {
    count: u64,
    next: AtomicU64,
}

impl RoundRobin {
    /// A round-robin cycle over partitions `0..count`.
    ///
    /// # Panics
    ///
    /// Panics when `count` is zero: a cycle over no partitions cannot place anything.
    #[must_use]
    pub fn partitions(count: i32) -> Self {
        assert!(
            count > 0,
            "a round-robin cycle needs at least one partition"
        );
        Self {
            #[allow(clippy::cast_sign_loss)] // asserted positive above
            count: count as u64,
            next: AtomicU64::new(0),
        }
    }
}

impl<K: ContextKind> PublishTransform<K, KafkaOptions> for RoundRobin {
    // It reads nothing from the position, so it mounts on a reply and on a slot alike, and the
    // destination is not its business.
    type Destination = Reads;

    fn apply(&self, out: &mut Outgoing<'_>, options: &mut Option<KafkaOptions>, _cx: &K::View<'_>) {
        if out.headers().get(PARTITION_KEY_HEADER).is_some()
            || options.is_some_and(|options| options.partition_setting().is_some())
        {
            return;
        }
        // The cycle position is below `count`, which came from a positive `i32`, so it is one.
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let partition = (self.next.fetch_add(1, Ordering::Relaxed) % self.count) as i32;
        let placed = options.unwrap_or_default().partition(partition);
        *options = Some(placed);
    }
}
