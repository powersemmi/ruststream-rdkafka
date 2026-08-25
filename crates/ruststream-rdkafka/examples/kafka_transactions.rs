//! Transactional publishing from a handler: an order fans out into per-item shipment commands,
//! published all-or-nothing through a transactional publisher the runtime injects into the
//! handler (an `Out` parameter paired from the policy the include site names). Readers on
//! Kafka's default `read_committed` isolation see the whole fan-out or none of it, and the
//! transactional id fences a zombie instance the moment its replacement initializes.
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_transactions -- run
//! ```

use ruststream::codec::{Codec, JsonCodec};
use ruststream::runtime::{App, AppInfo, Ctx, HandlerResult, Out, RustStream};
use ruststream::{OutgoingMessage, Publisher, TransactionalPublisher, subscriber};
use ruststream_rdkafka::context::keys::Partition;
use ruststream_rdkafka::{
    Commit, KafkaBroker, KafkaEosPublish, KafkaError, KafkaPublish, KafkaTopic, PartitionLanes,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    id: u64,
    items: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ItemShipment {
    order_id: u64,
    item: String,
}

// --8<-- [start:fanout]
/// Publishes one shipment command per item, all-or-nothing: `commit` makes the whole batch
/// visible atomically to `read_committed` readers, and any failure aborts so shipments are
/// never half-visible.
async fn dispatch<P: TransactionalPublisher>(publisher: &P, order: &Order) -> Result<(), P::Error> {
    publisher.begin_transaction().await?;
    for item in &order.items {
        let command = ItemShipment {
            order_id: order.id,
            item: item.clone(),
        };
        let payload = JsonCodec.encode(&command).expect("serializable");
        let outgoing = OutgoingMessage::new("shipments", payload.as_ref());
        if let Err(err) = publisher.publish(outgoing).await {
            publisher.abort().await.ok();
            return Err(err);
        }
    }
    publisher.commit().await
}
// --8<-- [end:fanout]

// --8<-- [start:handler]
// The publisher is a handler parameter, not application state: `Out` pairs the policy attached
// at the include site once the subscription opens, before the first delivery, so the handler
// holds a live, already-fenced producer by construction.
#[subscriber("orders")]
async fn ship(order: &Order, Out(shipments): Out<impl TransactionalPublisher>) -> HandlerResult {
    if dispatch(shipments, order).await.is_err() {
        // Nothing became visible; ask for redelivery and try the whole fan-out again.
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}
// --8<-- [end:handler]

// --8<-- [start:partitions]
// Concurrent transactional handlers: one producer runs one transaction at a time, so a worker
// pool cannot share one publisher (a second `begin_transaction` is a `TransactionBusy` error,
// not a silent merge). Under the default partition lanes a partition processes serially on one
// lane, so a publisher per source partition gives every lane its own independent transaction -
// and the id set follows the topic's partitions, not the worker count, so zombie fencing
// survives `workers(n)` changes.
async fn issue<L: PartitionLanes>(
    publishers: &L,
    order: &Order,
    partition: i32,
) -> Result<(), KafkaError> {
    let publisher = publishers.for_partition(partition).await?;
    publisher.begin_transaction().await?;
    for item in &order.items {
        let line = ItemShipment {
            order_id: order.id,
            item: item.clone(),
        };
        let payload = JsonCodec.encode(&line).expect("serializable");
        let outgoing = OutgoingMessage::new("invoice-lines", payload.as_ref());
        if let Err(err) = publisher.publish(outgoing).await {
            publisher.abort().await.ok();
            return Err(err);
        }
    }
    publisher.commit().await
}

#[subscriber(
    KafkaTopic::new("billing").group("billing-svc").commit(Commit::Tracked),
    workers(4, by_key)
)]
async fn bill(
    order: &Order,
    // The delivery's source partition picks the lane's publisher; the key injects it as a
    // plain argument (the DI form of `ctx.context(keys::Partition)`).
    Ctx(partition): Ctx<Partition>,
    Out(invoices): Out<impl PartitionLanes>,
) -> HandlerResult {
    if issue(invoices, order, partition).await.is_err() {
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}
// --8<-- [end:partitions]

// --8<-- [start:eos]
// Exactly-once: every lane publishes into one shared EOS pipeline, and the pipeline commits
// the consumed offsets inside the producer transaction (send_offsets_to_transaction), so
// source positions move atomically with the published records. A crash or an aborted window
// rewinds both - the output topic never sees a duplicate. The subscription's
// `Commit::Transactional` names the pipeline id; the consumer stops committing on its own.
//
// A publishing handler just returns the value: the runtime encodes it, and the pipeline's
// reply publisher (wired below) pairs it with this delivery's consumed offset automatically.
#[subscriber(
    KafkaTopic::new("raw-orders")
        .group("enrich-svc")
        .commit(Commit::Transactional("enrich-svc-1".into())),
    publish("enriched-orders"),
    workers(4, by_key)
)]
async fn enrich(order: &Order) -> Order {
    order.clone()
}
// --8<-- [end:eos]

#[ruststream::app]
fn app() -> impl App {
    let broker = KafkaBroker::new(["localhost:9092"]).default_group("shipments-svc");
    RustStream::new(AppInfo::new("shipments", "0.1.0")).with_broker(broker, |b| {
        // --8<-- [start:id]
        // A publish policy is pure declaration - it holds no connection, so it is written at
        // the include site and the runtime pairs it into a live publisher after the broker
        // connects. The transactional id must be stable and unique per concurrent producer:
        // it is what fences a zombie instance. One id per service replica (pod ordinal,
        // instance id) is the usual scheme.
        b.include(ship)
            .publisher(KafkaPublish::default().transactional_id("shipments-svc-1"));
        // --8<-- [end:id]
        // `per_partition` makes the id the base of one id per source partition
        // ("billing-svc-1-p{partition}"), pairing into `TransactionalPartitions`.
        b.include(bill).publisher(
            KafkaPublish::default()
                .transactional_id("billing-svc-1")
                .per_partition(),
        );
        // --8<-- [start:eos_wiring]
        // Every reply of `enrich` rides the pipeline's window, paired with its offset. The
        // pipeline id doubles as the producer's transactional id, and the `enrich`
        // subscription names the same id in its `Commit::Transactional` mode.
        b.include(enrich)
            .publisher(KafkaEosPublish::new("enrich-svc-1").replies());
        // --8<-- [end:eos_wiring]
    })
}
