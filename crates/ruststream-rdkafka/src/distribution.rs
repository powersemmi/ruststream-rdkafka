//! Producer-side distribution policies built on the explicit-partition header.
//!
//! librdkafka's partitioner families (random, consistent, murmur2, fnv1a) cannot express
//! per-message round-robin, and keyless distribution may batch-stick to one partition. For
//! workloads with long, near-constant per-message processing times that unevenness turns into
//! one hot consumer and idle peers; [`RoundRobin`] stamps each outgoing reply with the next
//! partition in the cycle instead.

use std::sync::atomic::{AtomicU64, Ordering};

use ruststream::runtime::{Outgoing, PublishContext, PublishTransform};

use crate::message::{PARTITION_HEADER, PARTITION_KEY_HEADER};

/// A [`PublishTransform`] distributing replies round-robin across the first `count` partitions.
///
/// Each reply gets the [`PARTITION_HEADER`] of an incrementing counter modulo `count`, so the
/// publisher targets partitions 0..count in a cycle, one message each - the evenest possible
/// spread for long, near-constant-cost messages. A reply that already carries an explicit
/// partition or a record key is left alone: keys exist for ordering, and overriding either
/// would silently break the caller's placement.
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
/// # #[derive(serde::Serialize)]
/// # struct WorkItem { order_id: u64 }
///
/// #[ruststream::subscriber("orders", publish("work-items"))]
/// async fn plan(order: &Order) -> WorkItem {
///     WorkItem { order_id: order.id }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("planner", "0.1.0"))
///         .with_broker(KafkaBroker::new(["localhost:9092"]), |b| {
///             b.include(plan)
///                 .publisher(Publish::default())
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

impl<C> PublishTransform<C> for RoundRobin {
    fn apply(&self, out: &mut Outgoing<'_>, _cx: &PublishContext<'_, C>) {
        if out.headers().get(PARTITION_HEADER).is_some()
            || out.headers().get(PARTITION_KEY_HEADER).is_some()
        {
            return;
        }
        let slot = self.next.fetch_add(1, Ordering::Relaxed) % self.count;
        out.headers_mut().insert(PARTITION_HEADER, slot.to_string());
    }
}
