//! Where a retry copy goes when the subscription reads more than one topic.
//!
//! A subscription over a set of topics addresses no copy of its own, so the mount site names the
//! destination. [`ToSourceTopic`] is the answer that keeps a retry on the topic it belongs to;
//! `.to(topic)` is the one for a fixed retry topic.

use ruststream::runtime::{ForReply, Names, Outgoing, PublishContext, PublishTransform};

use crate::context::KafkaContext;
use crate::context::keys::Topic;

/// A [`PublishTransform`] sending each retry copy back to the topic its delivery arrived on.
///
/// A [`KafkaTopics`](crate::KafkaTopics) subscription reads several topics, and a
/// [`KafkaPartitions`](crate::KafkaPartitions) reader reads part of one, so neither addresses its
/// own retry copies and every registration over them names where a copy goes. Naming one fixed
/// topic with `.to(topic)` sends an `orders-eu` record's retry to that topic whatever it was;
/// this transform reads the record's own topic out of the delivery instead, so a retry stays
/// where it belongs and the handler sees it arrive under the same name.
///
/// It writes no per-record setting, so it mounts over any Kafka publisher, and it takes the
/// destination, so it mounts where nothing else has declared one - a retry position with no
/// `.to(topic)`.
///
/// The topic comes from [`KafkaContext`], which reaches the position when the handler reads that
/// context (any `Ctx<..>` key of [`context::keys`](crate::context)). A batch handler carries
/// [`KafkaBatchContext`](crate::context::KafkaBatchContext) instead, which holds no per-record
/// topic, so a batch registration names its destination with `.to(topic)`.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream_rdkafka::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64 }
///
/// /// Reading the source topic is also what puts the Kafka context on this registration, which
/// /// is what the transform reads.
/// #[ruststream::subscriber(KafkaTopics::new(["orders-eu", "orders-us"]).group("orders-svc"))]
/// async fn place(order: &Order, Ctx(topic): Ctx<Topic>) -> HandlerOutcome {
///     println!("order {} from {topic}", order.id);
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .with_broker(KafkaBroker::new(["localhost:9092"]), |b| {
///             b.include(place)
///                 .max_attempts(nonzero!(5u32))
///                 .dead_letter("orders.dlq")
///                 .out_retry(Publish::default())
///                 .transform(ToSourceTopic);
///         })
/// }
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ToSourceTopic;

impl<Options> PublishTransform<ForReply<KafkaContext>, Options> for ToSourceTopic {
    // Naming the destination is the whole point, so it mounts only where nothing else has.
    type Destination = Names;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, KafkaContext>,
    ) {
        out.set_name(cx.context(Topic).to_owned());
    }
}
