//! The billing service of the Schema Registry guide: Avro invoices in, Avro receipts out, JSON
//! Schema disputes in, all under the Confluent envelope; and an archive reader that pins one
//! Avro schema with no registry.
//!
//! ```text
//! docker compose up -d
//! cargo run --example guide_schema_registry --features avro,testing -- run
//! ```

use apache_avro::AvroSchema;
use ruststream::codec::JsonCodec;
use ruststream_rdkafka::avro::AvroCodec;
use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::schema_registry::JsonSchema;
use ruststream_rdkafka::{KafkaPublish, SchemaFramed, SchemaPrefetch, SchemaRegistry};
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

// --8<-- [start:models]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, AvroSchema)]
struct Invoice {
    id: i64,
    amount_cents: i64,
    // Added in the second version of the schema; older producers do not write it.
    #[avro(default = r#""EUR""#)]
    currency: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, AvroSchema, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    invoice: i64,
    amount_cents: i64,
    currency: String,
}
// --8<-- [end:models]

// --8<-- [start:handler]
#[subscriber("invoices", reply)]
async fn issue_receipt(invoice: &Invoice) -> Receipt {
    tracing::info!(invoice.id, invoice.amount_cents, "receipt issued");
    Receipt {
        invoice: invoice.id,
        amount_cents: invoice.amount_cents,
        currency: invoice.currency.clone(),
    }
}
// --8<-- [end:handler]

// --8<-- [start:dispute]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
struct Dispute {
    invoice: i64,
    reason: String,
}

#[subscriber("disputes")]
async fn open_dispute(dispute: &Dispute) -> HandlerOutcome {
    if dispute.reason.is_empty() {
        tracing::warn!(dispute.invoice, "dispute without a reason dropped");
        return HandlerOutcome::drop();
    }
    tracing::info!(dispute.invoice, dispute.reason, "dispute opened");
    HandlerOutcome::ack()
}
// --8<-- [end:dispute]

// --8<-- [start:service]
fn service(broker: KafkaBroker, registry: SchemaRegistry) -> impl App {
    let prefetch = SchemaPrefetch::new(registry);
    let avro = AvroCodec::registry(&prefetch)
        .register::<Receipt>("receipts-value")
        .resolve_onto(Invoice::get_schema())
        .expect("a registry codec resolves its reader schema on the first delivery");
    let json = SchemaFramed::new(&prefetch, JsonCodec);

    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker_codec(
        broker.schema_prefetch(prefetch),
        avro.clone(),
        |b| {
            b.include(issue_receipt)
                .out_reply(KafkaPublish::default())
                .codec(avro);
            b.include_router(Router::new().with_codec(json).include(open_dispute));
        },
    )
}

#[ruststream::app]
fn app() -> impl App {
    service(
        KafkaBroker::new(["localhost:9092"]).default_group("billing-svc"),
        SchemaRegistry::new("http://localhost:8081"),
    )
}
// --8<-- [end:service]

#[cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "a second service the guide shows; its test runs it"
    )
)]
// --8<-- [start:archive]
fn archive(broker: KafkaBroker) -> impl App {
    let pinned = AvroCodec::local(Invoice::get_schema()).expect("the invoice schema resolves");
    RustStream::new(AppInfo::new("billing-archive", "0.1.0")).with_broker_codec(
        broker,
        pinned,
        |b| {
            b.include(archive_invoice);
        },
    )
}

#[subscriber("invoices.archive")]
async fn archive_invoice(invoice: &Invoice) -> HandlerOutcome {
    tracing::info!(invoice.id, "invoice archived");
    HandlerOutcome::ack()
}
// --8<-- [end:archive]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::process;

    use apache_avro::AvroSchema;
    use ruststream::codec::JsonCodec;
    use ruststream::testing::TestApp;
    use ruststream_rdkafka::avro::AvroCodec;
    use ruststream_rdkafka::prelude::*;
    use ruststream_rdkafka::schema_registry::JsonSchema;
    use ruststream_rdkafka::{SchemaFramed, SchemaPrefetch, SchemaRegistry};
    use serde::{Deserialize, Serialize};

    use super::{Dispute, Invoice, Receipt, archive, live, service};

    /// An invoice as the invoicing service writes it today.
    #[derive(Debug, Serialize, AvroSchema, Outgoing)]
    #[serde(rename = "Invoice")]
    #[outgoing(name = "invoices")]
    struct Issued {
        id: i64,
        amount_cents: i64,
        currency: String,
    }

    /// An invoice as a producer still on the first version of the schema writes it.
    #[derive(Debug, Serialize, AvroSchema, Outgoing)]
    #[serde(rename = "Invoice")]
    #[outgoing(name = "invoices")]
    struct IssuedV1 {
        id: i64,
        amount_cents: i64,
    }

    /// A receipt model that changed the type of a field without asking the registry.
    #[derive(Serialize, Deserialize, AvroSchema)]
    #[serde(rename = "Receipt")]
    struct DriftedReceipt {
        invoice: String,
        amount_cents: i64,
        currency: String,
    }

    /// A dispute as the support desk publishes it.
    #[derive(Serialize, JsonSchema, Outgoing)]
    #[serde(rename = "Dispute")]
    #[outgoing(name = "disputes")]
    struct Filed {
        invoice: i64,
        reason: String,
    }

    /// An archived invoice, a bare Avro datum.
    #[derive(Serialize, AvroSchema, Outgoing)]
    #[serde(rename = "Invoice")]
    #[outgoing(name = "invoices.archive")]
    struct Archived {
        id: i64,
        amount_cents: i64,
        currency: String,
    }

    fn broker() -> KafkaBroker {
        KafkaBroker::new(["localhost:9092"]).default_group("billing-svc")
    }

    /// The stand's registry, inside a context of this test's own: the service names its subjects
    /// as production does, and no other run or test sees them.
    fn registry(test: &str) -> Option<SchemaRegistry> {
        let url = live::url("SCHEMA_REGISTRY_TEST_URL")?;
        Some(SchemaRegistry::new(format!(
            "{url}/contexts/.guide-schema-registry-{}-{test}",
            process::id(),
        )))
    }

    #[tokio::test]
    async fn an_invoice_gets_a_receipt_under_its_subject() -> Result<(), Box<dyn Error>> {
        let Some(registry) = registry("receipt") else {
            return Ok(());
        };
        registry.register_avro::<Issued>("invoices-value").await?;
        registry.register_avro::<Receipt>("receipts-value").await?;
        let tb = TestApp::start(service(broker(), registry.clone())).await?;

        let prefetch = SchemaPrefetch::new(registry);
        let writer = AvroCodec::registry(&prefetch).register::<Issued>("invoices-value");
        tb.broker::<KafkaBroker>()
            .message(&Issued {
                id: 7,
                amount_cents: 1250,
                currency: "USD".to_owned(),
            })
            .with_codec(writer.clone())
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("invoices")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<KafkaBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with_codec(
                &writer,
                &Receipt {
                    invoice: 7,
                    amount_cents: 1250,
                    currency: "USD".to_owned(),
                },
            );
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_invoice_from_an_older_producer_reads_with_the_default_currency()
    -> Result<(), Box<dyn Error>> {
        let Some(registry) = registry("older") else {
            return Ok(());
        };
        registry.register_avro::<IssuedV1>("invoices-value").await?;
        registry.register_avro::<Receipt>("receipts-value").await?;
        let tb = TestApp::start(service(broker(), registry.clone())).await?;

        let prefetch = SchemaPrefetch::new(registry);
        let writer = AvroCodec::registry(&prefetch).register::<IssuedV1>("invoices-value");
        tb.broker::<KafkaBroker>()
            .message(&IssuedV1 {
                id: 8,
                amount_cents: 990,
            })
            .with_codec(writer.clone())
            .publish()
            .await?;

        let reader = AvroCodec::registry(&prefetch).resolve_onto(Invoice::get_schema())?;
        tb.broker::<KafkaBroker>()
            .subscriber("invoices")
            .assert_called_once()
            .with_codec(
                &reader,
                &Invoice {
                    id: 8,
                    amount_cents: 990,
                    currency: "EUR".to_owned(),
                },
            );
        tb.broker::<KafkaBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with_codec(
                &writer,
                &Receipt {
                    invoice: 8,
                    amount_cents: 990,
                    currency: "EUR".to_owned(),
                },
            );
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_missing_receipts_subject_stops_the_service() {
        let Some(registry) = registry("missing") else {
            return;
        };

        let Err(refused) = TestApp::start(service(broker(), registry)).await else {
            panic!("the service started without the subject its receipts publish under");
        };
        assert!(
            refused.to_string().contains("receipts-value"),
            "the error names the subject: {refused}",
        );
    }

    #[tokio::test]
    async fn a_receipt_model_the_registry_would_refuse_stops_the_service()
    -> Result<(), Box<dyn Error>> {
        let Some(registry) = registry("drift") else {
            return Ok(());
        };
        registry
            .register_avro::<DriftedReceipt>("receipts-value")
            .await?;

        let Err(refused) = TestApp::start(service(broker(), registry)).await else {
            panic!("the service started with a receipt model its subject does not accept");
        };
        assert!(
            refused.to_string().contains("not compatible"),
            "the registry's account travels in the error: {refused}",
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_dispute_reads_through_the_json_schema_envelope() -> Result<(), Box<dyn Error>> {
        let Some(registry) = registry("dispute") else {
            return Ok(());
        };
        registry.register_avro::<Receipt>("receipts-value").await?;
        registry.register_json::<Filed>("disputes-value").await?;
        let tb = TestApp::start(service(broker(), registry.clone())).await?;

        let prefetch = SchemaPrefetch::new(registry);
        let writer = SchemaFramed::new(&prefetch, JsonCodec).register::<Filed>("disputes-value");
        tb.broker::<KafkaBroker>()
            .message(&Filed {
                invoice: 7,
                reason: "charged twice".to_owned(),
            })
            .with_codec(writer.clone())
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("disputes")
            .assert_called_once()
            .with_codec(
                &writer,
                &Dispute {
                    invoice: 7,
                    reason: "charged twice".to_owned(),
                },
            )
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_archived_invoice_reads_with_the_pinned_schema() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(archive(broker())).await?;

        let pinned = AvroCodec::local(Invoice::get_schema())?;
        tb.broker::<KafkaBroker>()
            .message(&Archived {
                id: 3,
                amount_cents: 400,
                currency: "EUR".to_owned(),
            })
            .with_codec(pinned.clone())
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("invoices.archive")
            .assert_called_once()
            .with_codec(
                &pinned,
                &Invoice {
                    id: 3,
                    amount_cents: 400,
                    currency: "EUR".to_owned(),
                },
            )
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
