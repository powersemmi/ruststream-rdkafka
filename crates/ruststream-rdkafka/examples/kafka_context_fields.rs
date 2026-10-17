//! What a Kafka delivery carries besides its payload and headers: the topic, the partition, the
//! offset, the timestamp and the record key, each bound as a handler parameter with `Ctx<K>`.
//!
//! ```text
//! cargo test --example kafka_context_fields --features testing
//! cargo run --example kafka_context_fields -- run
//! ```

use bytes::Bytes;
use ruststream_rdkafka::context::keys::{Key, Offset, TimestampMillis};
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

/// Where an order sits in the log, written to the audit trail.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "audit-trail")]
struct AuditRecord {
    order_id: u64,
    topic: String,
    partition: i32,
    offset: i64,
    timestamp_millis: Option<i64>,
    customer: Option<String>,
}

// --8<-- [start:context]
/// Each `Ctx<K>` binds one field of the record; the order's customer is its record key.
#[subscriber(KafkaTopic::new("orders").group("audit"), reply)]
async fn audit(
    order: &Order,
    Ctx(topic): Ctx<Topic>,
    Ctx(partition): Ctx<Partition>,
    Ctx(offset): Ctx<Offset>,
    Ctx(timestamp_millis): Ctx<TimestampMillis>,
    Ctx(key): Ctx<Key>,
) -> AuditRecord {
    let customer = key.map(|key: Bytes| String::from_utf8_lossy(&key).into_owned());
    tracing::info!(order.id, %topic, partition, offset, "order audited");
    AuditRecord {
        order_id: order.id,
        topic: topic.to_string(),
        partition,
        offset,
        timestamp_millis,
        customer,
    }
}
// --8<-- [end:context]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]),
        |b| {
            b.include(audit).out_reply(Publish::default());
        },
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::HeaderMap;
    use ruststream::testing::TestApp;
    use ruststream_rdkafka::PARTITION_KEY_HEADER;
    use ruststream_rdkafka::prelude::*;

    use super::{AuditRecord, Order, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_handler_reads_where_each_record_sits() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KafkaBroker>()
            .message(&Order { id: 1 })
            .publish()
            .await?;
        let mut keyed = HeaderMap::new();
        keyed.insert(PARTITION_KEY_HEADER, "customer-42");
        tb.broker::<KafkaBroker>()
            .message(&Order { id: 2 })
            .with_headers(keyed)
            .publish()
            .await?;

        let records = tb
            .broker::<KafkaBroker>()
            .published::<AuditRecord>("audit-trail")
            .assert_called(2)
            .decoded();
        let located: Vec<_> = records
            .iter()
            .map(|record| {
                (
                    record.order_id,
                    record.topic.as_str(),
                    record.partition,
                    record.offset,
                    record.customer.as_deref(),
                )
            })
            .collect();
        assert_eq!(
            located,
            [
                (1, "orders", 0, 0, None),
                (2, "orders", 0, 1, Some("customer-42")),
            ],
        );
        assert!(
            records
                .iter()
                .all(|record| record.timestamp_millis.is_some()),
            "every record carries its create time",
        );
        tb.shutdown().await?;
        Ok(())
    }
}
