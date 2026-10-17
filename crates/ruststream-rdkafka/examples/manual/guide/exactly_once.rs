//! The service of the Transactions and exactly-once guide, written without the macros: each
//! handler is a type with an `impl Handle`, a slot is one entry of the body's `Outs`, and `main`
//! is written by hand.
//!
//! ```text
//! docker compose up -d
//! cargo run --example manual_guide_exactly_once -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_rdkafka::context::KafkaContext;
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../../tests/live/mod.rs"]
mod live;

// --8<-- [start:fanout]
#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    items: Vec<Item>,
}

#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Item {
    sku: String,
    quantity: u32,
}

/// One shipment line per item of a paid order.
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct ShipmentLine {
    order_id: u64,
    sku: String,
    quantity: u32,
}

// `#[derive(Outgoing)]` with `#[outgoing(name = "shipment-lines")]`, by hand.
impl OutgoingDestination for ShipmentLine {
    type Form = FixedName;

    const DESTINATION: &'static str = "shipment-lines";
}

impl MessageHeaders for ShipmentLine {
    type Contract = NoHeaders;
}

impl MessageInfo for ShipmentLine {
    const NAME: &'static str = "ShipmentLine";
    const DESCRIPTION: Option<&'static str> = None;
}

/// The slot the shipment lines leave through.
struct Shipments;

impl OutSlot for Shipments {
    const NAME: &'static str = "Shipments";
    // `ShipmentLine` fixes its destination, so no transform on this slot may name one.
    type Destination = Reads;
}

impl PublishedThrough<Shipments> for ShipmentLine {}

struct Ship;

impl<S> Handle<Order, (), Outs<(S,)>> for Ship
where
    S: OutEntry<Shipments, Wire: TransactionalPublisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(S,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let shipments = outs.get(Shipments);
        if shipments.begin_transaction().await.is_err() {
            return Err(HandlerOutcome::retry());
        }
        for item in &order.items {
            if item.quantity == 0 {
                tracing::warn!(order.id, sku = %item.sku, "order with an empty line dropped");
                return match shipments.abort().await {
                    Ok(()) => Err(HandlerOutcome::drop()),
                    Err(_) => Err(HandlerOutcome::retry()),
                };
            }
            let line = ShipmentLine {
                order_id: order.id,
                sku: item.sku.clone(),
                quantity: item.quantity,
            };
            if shipments.message(&line).publish().await.is_err() {
                shipments.abort().await.ok();
                return Err(HandlerOutcome::retry());
            }
        }
        if shipments.commit().await.is_err() {
            return Err(HandlerOutcome::retry());
        }
        tracing::info!(order.id, lines = order.items.len(), "order shipped");
        Ok(())
    }
}
// --8<-- [end:fanout]

// --8<-- [start:lanes]
/// One invoice line per item of a billed order.
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct InvoiceItem {
    order_id: u64,
    sku: String,
    quantity: u32,
}

impl OutgoingDestination for InvoiceItem {
    type Form = FixedName;

    const DESTINATION: &'static str = "invoice-items";
}

impl MessageHeaders for InvoiceItem {
    type Contract = NoHeaders;
}

impl MessageInfo for InvoiceItem {
    const NAME: &'static str = "InvoiceItem";
    const DESCRIPTION: Option<&'static str> = None;
}

/// Declaring `KafkaContext` is what gives the body the delivery's partition.
struct Invoice;

impl<L> Handle<Order, (), Outs<(L,)>, KafkaContext> for Invoice
where
    L: OutEntry<DefaultSlot, Wire: PartitionLanes>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(L,)>,
        ctx: &mut Context<'_, KafkaContext>,
    ) -> Result<(), HandlerOutcome> {
        let partition = ctx.context(Partition);
        let Ok(items) = outs.get(DefaultSlot).for_partition(partition).await else {
            return Err(HandlerOutcome::retry());
        };
        if items.begin_transaction().await.is_err() {
            return Err(HandlerOutcome::retry());
        }
        for item in &order.items {
            let line = InvoiceItem {
                order_id: order.id,
                sku: item.sku.clone(),
                quantity: item.quantity,
            };
            if items.message(&line).publish().await.is_err() {
                items.abort().await.ok();
                return Err(HandlerOutcome::retry());
            }
        }
        if items.commit().await.is_err() {
            return Err(HandlerOutcome::retry());
        }
        tracing::info!(order.id, partition, "order invoiced");
        Ok(())
    }
}
// --8<-- [end:lanes]

// --8<-- [start:eos]
/// A placed order with the totals the warehouse plans with.
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct EnrichedOrder {
    id: u64,
    lines: usize,
    units: u32,
}

impl OutgoingDestination for EnrichedOrder {
    type Form = FixedName;

    const DESTINATION: &'static str = "enriched-orders";
}

impl MessageHeaders for EnrichedOrder {
    type Contract = NoHeaders;
}

impl MessageInfo for EnrichedOrder {
    const NAME: &'static str = "EnrichedOrder";
    const DESCRIPTION: Option<&'static str> = None;
}

struct Enrich;

impl Handle<Order, EnrichedOrder> for Enrich {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<EnrichedOrder, HandlerOutcome>> {
        let units = order.items.iter().map(|item| item.quantity).sum();
        tracing::info!(order.id, units, "order enriched");
        ready(Ok(EnrichedOrder {
            id: order.id,
            lines: order.items.len(),
            units,
        }))
    }
}
// --8<-- [end:eos]

// --8<-- [start:service]
fn service(broker: KafkaBroker) -> impl App {
    RustStream::new(AppInfo::new("fulfilment", "0.1.0")).with_broker(broker, |b| {
        // One transactional id per service instance.
        let paid = subscriber("paid-orders", Ship).map_source(|_| {
            KafkaTopic::new("paid-orders")
                .group("shipping")
                .commit(Commit::Tracked)
        });
        b.include(paid.build())
            .out(Shipments, Publish::default().transactional_id("shipping-1"))
            .build();
        // One transactional id per source partition: "invoicing-1-p0", "invoicing-1-p1", ...
        let billable = subscriber("billable-orders", Invoice)
            .map_source(|_| {
                KafkaTopic::new("billable-orders")
                    .group("invoicing")
                    .commit(Commit::Tracked)
            })
            .workers_by_key(nonzero!(4));
        b.include(billable.build())
            .out(
                DefaultSlot,
                Publish::default()
                    .transactional_id("invoicing-1")
                    .per_partition(),
            )
            .build();
        // The pipeline's id is the one the subscription's commit mode names.
        let placed = subscriber("placed-orders", Enrich)
            .map_source(|_| {
                KafkaTopic::new("placed-orders")
                    .group("enrichment")
                    .commit(Commit::Transactional("enrichment-1".into()))
            })
            .reply()
            .workers_by_key(nonzero!(4));
        b.include(placed.build())
            .out_reply(EosPublish::new("enrichment-1"))
            .transform(EosReplies);
    })
}

fn app() -> impl App {
    service(KafkaBroker::new(["localhost:9092"]))
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::process;
    use std::time::{Duration, Instant};

    use rdkafka::ClientConfig;
    use rdkafka::Message as _;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::consumer::{BaseConsumer, Consumer as _};
    use ruststream::testing::{Outcome, TestApp};
    use ruststream_rdkafka::PARTITION_KEY_HEADER;
    use ruststream_rdkafka::prelude::*;
    use serde::Serialize;

    use super::{EnrichedOrder, InvoiceItem, ShipmentLine, Shipments, app, live, service};

    /// An order as the shop publishes it; the call names the topic.
    #[derive(Serialize)]
    struct Placed {
        id: u64,
        items: Vec<Line>,
    }

    impl OutgoingDestination for Placed {
        type Form = CallerName;
    }

    impl MessageHeaders for Placed {
        type Contract = NoHeaders;
    }

    impl MessageInfo for Placed {
        const NAME: &'static str = "Placed";
        const DESCRIPTION: Option<&'static str> = None;
    }

    #[derive(Serialize)]
    struct Line {
        sku: &'static str,
        quantity: u32,
    }

    fn order(id: u64, quantities: &[u32]) -> Placed {
        let skus = ["mug", "lamp", "desk"];
        Placed {
            id,
            items: quantities
                .iter()
                .zip(skus)
                .map(|(&quantity, sku)| Line { sku, quantity })
                .collect(),
        }
    }

    #[tokio::test]
    async fn an_order_ships_every_line_or_none() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<KafkaBroker>();

        // The second line is empty: the first one was sent into the transaction, then aborted.
        broker
            .message(&order(1, &[2, 0]))
            .to("paid-orders")
            .publish()
            .await?;
        tb.out::<Shipments>().assert_called(1);
        broker
            .published::<ShipmentLine>("shipment-lines")
            .assert_not_called();

        broker
            .message(&order(2, &[2, 1]))
            .to("paid-orders")
            .publish()
            .await?;
        broker
            .published::<ShipmentLine>("shipment-lines")
            .assert_called(2)
            .with(&ShipmentLine {
                order_id: 2,
                sku: "lamp".to_owned(),
                quantity: 1,
            });
        assert_eq!(
            broker.subscriber("paid-orders").outcomes(),
            [Outcome::Drop, Outcome::Ack],
        );
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_lane_invoices_in_its_partitions_transaction() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<KafkaBroker>();

        broker
            .message(&order(3, &[1, 4]))
            .to("billable-orders")
            .publish()
            .await?;

        broker
            .published::<InvoiceItem>("invoice-items")
            .assert_called(2)
            .with(&InvoiceItem {
                order_id: 3,
                sku: "lamp".to_owned(),
                quantity: 4,
            });
        broker
            .subscriber("billable-orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    /// The reply joins the pipeline's window and lands when the window commits, together with
    /// the consumed offset; the publish drives the reaction through that commit.
    #[tokio::test(start_paused = true)]
    async fn an_enriched_order_lands_when_its_window_commits() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<KafkaBroker>();

        broker
            .message(&order(4, &[2, 3]))
            .to("placed-orders")
            .publish()
            .await?;

        broker
            .published::<EnrichedOrder>("enriched-orders")
            .assert_called_once()
            .with(&EnrichedOrder {
                id: 4,
                lines: 2,
                units: 5,
            });
        broker
            .subscriber("placed-orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    /// Two partitions, two lanes, two transactional ids: both orders commit on the first try,
    /// so neither lane found the other's transaction open on its producer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_two_partitions_invoice_in_their_own_transactions() {
        let Some(url) = live::url("KAFKA_TEST_URL") else {
            return;
        };
        recreate_topic(&url, "billable-orders", 2).await;
        recreate_topic(&url, "invoice-items", 1).await;
        let tb = TestApp::start_live_within(
            service(KafkaBroker::new([url.as_str()])),
            Duration::from_secs(30),
        )
        .await
        .expect("start live");
        let broker = tb.broker::<KafkaBroker>();
        // librdkafka's partitioner puts these record keys on partitions 0 and 1 of two.
        for (id, key) in [(5, "invoice-4"), (6, "invoice-1")] {
            let mut headers = HeaderMap::new();
            headers.insert(PARTITION_KEY_HEADER, key);
            broker
                .message(&order(id, &[1, 2]))
                .to("billable-orders")
                .with_headers(headers)
                .publish()
                .await
                .expect("publish");
        }

        assert_eq!(
            broker.subscriber("billable-orders").outcomes(),
            [Outcome::Ack, Outcome::Ack],
        );
        tb.shutdown().await.expect("shutdown");

        // A lane's publisher is outside the harness's record, so the committed items are read
        // off the topic the way a `read_committed` consumer reads them.
        let mut items = committed_items(&url, 4);
        items.sort_by_key(|item| (item.order_id, item.quantity));
        let expected: Vec<(u64, u32)> = vec![(5, 1), (5, 2), (6, 1), (6, 2)];
        let read: Vec<(u64, u32)> = items
            .iter()
            .map(|item| (item.order_id, item.quantity))
            .collect();
        assert_eq!(read, expected);
    }

    /// The first `count` records a `read_committed` consumer reads from `invoice-items`.
    fn committed_items(url: &str, count: usize) -> Vec<InvoiceItem> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", url)
            .set(
                "group.id",
                format!("invoice-items-reader-{}", process::id()),
            )
            .set("auto.offset.reset", "earliest")
            .set("isolation.level", "read_committed")
            .create()
            .expect("reader");
        consumer
            .subscribe(&["invoice-items"])
            .expect("subscribe the reader");
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut items = Vec::new();
        while items.len() < count {
            assert!(Instant::now() < deadline, "only {items:?} were committed");
            // Each poll returns as soon as a record arrives, so the loop is paced by the topic.
            if let Some(record) = consumer.poll(Duration::from_millis(500)) {
                let record = record.expect("a record");
                let payload = record.payload().expect("a payload");
                items.push(serde_json::from_slice(payload).expect("an invoice item"));
            }
        }
        items
    }

    /// Deletes `topic` and creates it again with `partitions` partitions, so the run reads only
    /// what it publishes.
    async fn recreate_topic(url: &str, topic: &str, partitions: i32) {
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", url)
            .create()
            .expect("admin client");
        let options = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
        let _ = admin.delete_topics(&[topic], &options).await;
        // Deletion completes asynchronously, and a create racing it fails with "already
        // exists"; each attempt waits for the cluster's answer, so the loop is paced by it.
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let created = admin
                .create_topics(
                    &[NewTopic::new(topic, partitions, TopicReplication::Fixed(1))],
                    &options,
                )
                .await
                .expect("create_topics call");
            if created.iter().all(Result::is_ok) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "{topic} was not recreated: {created:?}"
            );
        }
    }
}
