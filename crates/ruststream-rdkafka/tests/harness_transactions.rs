//! Services under `TestApp` that publish inside Kafka transactions: the production app on
//! `KafkaBroker`, connected in process.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::runtime::{AppInfo, Ctx, DefaultSlot, HandlerOutcome, Out, RustStream};
use ruststream::subscriber;
use ruststream::testing::TestApp;
use ruststream::{OutSlot, Outgoing, OutgoingMessage, Publisher, TransactionalPublisher};
use ruststream_rdkafka::context::keys::Partition;
use ruststream_rdkafka::{
    Commit, EosReplies, KafkaBroker, KafkaEosPublish, KafkaPublish, KafkaTopic, PartitionLanes,
};
use serde::{Deserialize, Serialize};

/// The broker every app here runs on, configured as a service configures it.
fn broker() -> KafkaBroker {
    KafkaBroker::new(["kafka:9092"]).default_group("svc")
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Order {
    id: u64,
}

#[derive(Debug, Serialize, Deserialize, Outgoing)]
struct PlanOrder {
    id: u64,
}

#[derive(Debug, Serialize, Deserialize, Outgoing)]
struct PlanItem {
    order_id: u64,
}

// ----------------------------------------------------------------------------- transactions

#[derive(Debug, Serialize, Deserialize, Outgoing)]
struct Fanout {
    id: u64,
    items: u64,
    /// Whether the handler commits its fan-out or aborts it, so one mount covers both paths.
    commit: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
    item: u64,
}

#[derive(OutSlot)]
#[publishes(Shipment)]
struct Shipments;

#[subscriber("fanout-orders")]
async fn fan_out(
    order: &Fanout,
    Out(shipments): Out<impl TransactionalPublisher, Shipments>,
) -> HandlerOutcome {
    if shipments.begin_transaction().await.is_err() {
        return HandlerOutcome::retry();
    }
    for item in 0..order.items {
        let line = Shipment {
            order_id: order.id,
            item,
        };
        if shipments.message(&line).publish().await.is_err() {
            shipments.abort().await.ok();
            return HandlerOutcome::retry();
        }
    }
    let settled = if order.commit {
        shipments.commit().await
    } else {
        shipments.abort().await
    };
    if settled.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The topic shows what a transaction committed; the slot records what the handler sent through
/// it, aborted or not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transactional_slot_publishes_only_what_it_commits() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(), |b| {
        b.include(fan_out)
            .out(
                Shipments,
                KafkaPublish::default().transactional_id("shipments-svc-1"),
            )
            .build();
    });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaBroker>()
        .message(&Fanout {
            id: 1,
            items: 2,
            commit: false,
        })
        .to("fanout-orders")
        .publish()
        .await
        .expect("publish");
    tb.broker::<KafkaBroker>()
        .published::<Shipment>("shipments")
        .assert_not_called();
    tb.out::<Shipments>().assert_called(2);

    tb.broker::<KafkaBroker>()
        .message(&Fanout {
            id: 2,
            items: 2,
            commit: true,
        })
        .to("fanout-orders")
        .publish()
        .await
        .expect("publish");
    tb.broker::<KafkaBroker>()
        .published::<Shipment>("shipments")
        .assert_called(2)
        .with(&Shipment {
            order_id: 2,
            item: 1,
        });
    tb.broker::<KafkaBroker>()
        .subscriber("fanout-orders")
        .assert_called(2)
        .settled(HandlerOutcome::ack());
    tb.shutdown().await.expect("shutdown");
}

#[subscriber("lane-orders")]
async fn bill_lane(
    order: &PlanOrder,
    Ctx(partition): Ctx<Partition>,
    Out(lanes): Out<impl PartitionLanes>,
) -> HandlerOutcome {
    let Ok(publisher) = lanes.for_partition(partition).await else {
        return HandlerOutcome::retry();
    };
    if publisher.begin_transaction().await.is_err() {
        return HandlerOutcome::retry();
    }
    let payload = format!(r#"{{"order_id":{}}}"#, order.id);
    if publisher
        .publish(OutgoingMessage::new("lane-items", payload.as_bytes()), None)
        .await
        .is_err()
    {
        publisher.abort().await.ok();
        return HandlerOutcome::retry();
    }
    if publisher.commit().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lane_publishes_through_its_partitions_transaction() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(), |b| {
        b.include(bill_lane)
            .out(
                DefaultSlot,
                KafkaPublish::default()
                    .transactional_id("lane-svc")
                    .per_partition(),
            )
            .build();
    });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaBroker>()
        .message(&PlanOrder { id: 9 })
        .to("lane-orders")
        .publish()
        .await
        .expect("publish");

    tb.broker::<KafkaBroker>()
        .published::<PlanItem>("lane-items")
        .assert_called_once();
    tb.broker::<KafkaBroker>()
        .subscriber("lane-orders")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    tb.shutdown().await.expect("shutdown");
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Enriched {
    id: u64,
    enriched: bool,
}

#[subscriber(
    KafkaTopic::new("eos-in").commit(Commit::Transactional("enrich-1".to_owned())),
    publish("eos-out")
)]
async fn enrich(order: &Order) -> Enriched {
    Enriched {
        id: order.id,
        enriched: true,
    }
}

/// An exactly-once reply joins the pipeline's window and becomes visible when the window commits,
/// together with the consumed offset. The publish drives the reaction through that commit.
#[tokio::test(start_paused = true)]
async fn an_exactly_once_reply_lands_when_its_window_commits() {
    let window = Duration::from_millis(100);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(), |b| {
        b.include(enrich)
            .out_reply(KafkaEosPublish::new("enrich-1").commit_interval(window))
            .transform(EosReplies);
    });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaBroker>()
        .message(&Order { id: 1 })
        .to("eos-in")
        .publish()
        .await
        .expect("publish");
    tb.broker::<KafkaBroker>()
        .subscriber("eos-in")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    tb.broker::<KafkaBroker>()
        .published::<Enriched>("eos-out")
        .assert_called_once()
        .with(&Enriched {
            id: 1,
            enriched: true,
        });
}
