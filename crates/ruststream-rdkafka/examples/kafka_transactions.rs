//! Transactional publishing from a handler: an order fans out into per-item shipment commands,
//! published all-or-nothing through a transactional publisher held in the typed application
//! state (the framework's DI: the handler declares `State<Shipments>` and the runtime injects
//! it). Readers on Kafka's default `read_committed` isolation see the whole fan-out or none of
//! it, and the transactional id fences a zombie instance the moment its replacement
//! initializes.
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_transactions -- run
//! ```

use std::convert::Infallible;

use ruststream::codec::{Codec, JsonCodec};
use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream, State};
use ruststream::{FromRef, OutgoingMessage, Publisher, TransactionalPublisher, subscriber};
use ruststream_rdkafka::context::{KafkaContext, keys};
use ruststream_rdkafka::{
    Commit, EosPipeline, KafkaBroker, KafkaError, KafkaPublisher, KafkaTopic,
    TransactionalPartitions,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: u64,
    items: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ItemShipment {
    order_id: u64,
    item: String,
}

// --8<-- [start:state]
// The application state wires the use-case object once at startup; `#[derive(FromRef)]` makes
// each field injectable into handlers as `State<FieldType>`.
#[derive(Clone, FromRef)]
struct AppState {
    shipments: Shipments,
    invoices: Invoices,
    enrichment: Enrichment,
}

#[derive(Clone)]
struct Shipments {
    publisher: KafkaPublisher,
}

impl Shipments {
    /// Publishes one shipment command per item, all-or-nothing: `commit` makes the whole batch
    /// visible atomically to `read_committed` readers, and any failure aborts so shipments are
    /// never half-visible.
    async fn dispatch(&self, order: &Order) -> Result<(), KafkaError> {
        self.publisher.begin_transaction().await?;
        for item in &order.items {
            let command = ItemShipment {
                order_id: order.id,
                item: item.clone(),
            };
            let payload = JsonCodec.encode(&command).expect("serializable");
            let outgoing = OutgoingMessage::new("shipments", payload.as_ref());
            if let Err(err) = self.publisher.publish(outgoing).await {
                self.publisher.abort().await.ok();
                return Err(err);
            }
        }
        self.publisher.commit().await
    }
}
// --8<-- [end:state]

// --8<-- [start:handler]
#[subscriber("orders")]
async fn ship(order: &Order, State(shipments): State<Shipments>) -> HandlerResult {
    if shipments.dispatch(order).await.is_err() {
        // Nothing became visible; ask for redelivery and try the whole fan-out again.
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}
// --8<-- [end:handler]

// --8<-- [start:partitions]
// Concurrent transactional handlers: one producer runs one transaction at a time, so a worker
// pool cannot share one publisher (a second `begin_transaction` is a `TransactionBusy` error,
// not a silent merge). Under the default partition lanes a partition processes serially on
// one lane, so a publisher per source partition gives every lane its own independent
// transaction - and the id set follows the topic's partitions, not the worker count, so
// zombie fencing survives `workers(n)` changes.
#[derive(Clone)]
struct Invoices {
    publishers: TransactionalPartitions,
}

impl Invoices {
    async fn issue(&self, order: &Order, partition: i32) -> Result<(), KafkaError> {
        let publisher = self.publishers.for_partition(partition);
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
}

#[subscriber(
    KafkaTopic::new("billing").group("billing-svc").commit(Commit::Tracked),
    workers(4, by_key)
)]
async fn bill(
    order: &Order,
    ctx: &mut Context<'_, KafkaContext>,
    State(invoices): State<Invoices>,
) -> HandlerResult {
    // The delivery's source partition picks the lane's publisher.
    let partition = ctx.context(keys::Partition);
    if invoices.issue(order, partition).await.is_err() {
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}
// --8<-- [end:partitions]

// --8<-- [start:eos]
// Exactly-once: every lane publishes into one shared EosPipeline, and the pipeline commits
// the consumed offsets inside the producer transaction (send_offsets_to_transaction), so
// source positions move atomically with the published records. A crash or an aborted window
// rewinds both - the output topic never sees a duplicate. The subscription's
// `Commit::Transactional` names the pipeline id; the consumer stops committing on its own.
#[derive(Clone)]
struct Enrichment {
    pipeline: EosPipeline,
}

#[subscriber(
    KafkaTopic::new("raw-orders")
        .group("enrich-svc")
        .commit(Commit::Transactional("enrich-svc-1".into())),
    workers(4, by_key)
)]
async fn enrich(
    order: &Order,
    ctx: &mut Context<'_, KafkaContext>,
    State(enrichment): State<Enrichment>,
) -> HandlerResult {
    // The delivery's source coordinates pair the published record with its consumed offset.
    let source = ctx.context(keys::Source);
    let payload = JsonCodec.encode(order).expect("serializable");
    let outgoing = OutgoingMessage::new("enriched-orders", payload.as_ref());
    if enrichment.pipeline.publish(source, outgoing).await.is_err() {
        // The window aborts and redelivers; nothing became visible downstream.
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}
// --8<-- [end:eos]

#[ruststream::app]
fn app() -> impl App {
    let broker = KafkaBroker::new(["localhost:9092"]).default_group("shipments-svc");
    // --8<-- [start:id]
    // The transactional id must be stable and unique per concurrent producer: it is what
    // fences a zombie instance. One id per service replica (pod ordinal, instance id) is the
    // usual scheme.
    let shipments = Shipments {
        publisher: broker.publisher().transactional_id("shipments-svc-1"),
    };
    // --8<-- [end:id]
    let invoices = Invoices {
        publishers: TransactionalPartitions::new(broker.publisher(), "billing-svc-1"),
    };
    // The pipeline id doubles as the producer's transactional id; the `enrich` subscription
    // names the same id in its `Commit::Transactional` mode.
    let enrichment = Enrichment {
        pipeline: EosPipeline::new(broker.publisher().transactional_id("enrich-svc-1")),
    };
    RustStream::new(AppInfo::new("shipments", "0.1.0"))
        .on_startup(move |()| async move {
            Ok::<_, Infallible>(AppState {
                shipments,
                invoices,
                enrichment,
            })
        })
        .with_broker(broker, |b| {
            b.include(ship);
            b.include(bill);
            b.include(enrich);
        })
}
