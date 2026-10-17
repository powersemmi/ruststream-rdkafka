//! The service of `examples/kafka_context_fields.rs` written without `#[subscriber]`: a body
//! typed over `KafkaContext` reads each field of the record off its context by its key.
//!
//! ```text
//! cargo test --example manual_kafka_context_fields --features testing
//! cargo run --example manual_kafka_context_fields -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_rdkafka::context::KafkaContext;
use ruststream_rdkafka::context::keys::{Key, Offset, TimestampMillis};
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

/// Where an order sits in the log, written to the audit trail.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
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
/// The body reads each field of the record off its context by its key; the order's customer is
/// its record key.
struct Audit;

impl Handle<Order, AuditRecord, (), KafkaContext> for Audit {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, KafkaContext>,
    ) -> impl Future<Output = Result<AuditRecord, HandlerOutcome>> {
        let topic = ctx.context(Topic).to_string();
        let partition = ctx.context(Partition);
        let offset = ctx.context(Offset);
        let customer = ctx
            .context(Key)
            .map(|key| String::from_utf8_lossy(key).into_owned());
        tracing::info!(order.id, %topic, partition, offset, "order audited");
        ready(Ok(AuditRecord {
            order_id: order.id,
            topic,
            partition,
            offset,
            timestamp_millis: ctx.context(TimestampMillis),
            customer,
        }))
    }
}

fn audited() -> KafkaTopic {
    KafkaTopic::new("orders").group("audit")
}
// --8<-- [end:context]

fn app() -> impl App {
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]),
        |b| {
            let audit = subscriber("orders", Audit).map_source(|_| audited());
            b.include(audit.reply().build())
                .out_reply(Publish::default());
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
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
