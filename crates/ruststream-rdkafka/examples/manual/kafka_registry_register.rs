//! The service of `examples/kafka_registry_register.rs` written without the macros.
//!
//! ```text
//! SCHEMA_REGISTRY_TEST_URL=http://127.0.0.1:8081 \
//!     cargo test --example manual_kafka_registry_register --features schema-registry,testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::schema_registry::JsonSchema;
use ruststream_rdkafka::{KafkaError, SchemaFrame, SchemaRegistry};
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
struct Payment {
    id: i64,
    amount: i64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
struct Invoice {
    payment: i64,
    amount: i64,
}

// `#[derive(Outgoing)]` with `#[outgoing(name = "invoices")]`.
impl OutgoingDestination for Invoice {
    type Form = FixedName;
    const DESTINATION: &'static str = "invoices";
}

impl MessageHeaders for Invoice {
    type Contract = NoHeaders;
}

/// The definition `#[subscriber("payments", reply)]` would have minted.
struct Bill;

impl Handle<Payment, Invoice> for Bill {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Invoice, HandlerOutcome>> {
        tracing::info!(payment.id, payment.amount, "invoice issued");
        ready(Ok(Invoice {
            payment: payment.id,
            amount: payment.amount,
        }))
    }
}

fn app(registry: SchemaRegistry) -> impl App {
    let broker = KafkaBroker::new(["localhost:9092"])
        .default_group("invoices-svc")
        .schema_registry(registry.clone());

    RustStream::new(AppInfo::new("invoices", "0.1.0"))
        .publish_layer(SchemaFrame::new(registry.clone()))
        .on_startup(async move |()| {
            // This service owns the invoices subject; an identical schema keeps its id.
            registry.register_json::<Invoice>("invoices-value").await?;
            // The payments subject belongs to its producer: warm checks it and registers nothing.
            registry.warm("payments-value").await?;
            Ok::<_, KafkaError>(())
        })
        .with_broker(broker, |b| {
            b.include(subscriber("payments", Bill).reply().build());
        })
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(|| app(SchemaRegistry::new("http://localhost:8081")))
}
// --8<-- [end:app]

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::schema_registry::JsonSchema;
    use ruststream_rdkafka::{SchemaRegistry, SchemaType};
    use serde::Serialize;

    use super::{app, live};

    /// The producer's model of a payment, which owns the `payments-value` subject.
    #[derive(Serialize, JsonSchema)]
    #[serde(rename = "Payment")]
    struct Produced {
        id: i64,
        amount: i64,
    }

    #[tokio::test]
    async fn startup_registers_one_subject_and_warms_the_other() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("SCHEMA_REGISTRY_TEST_URL") else {
            return Ok(());
        };
        let payments = SchemaRegistry::new(&url)
            .register_json::<Produced>("payments-value")
            .await?;
        let registry = SchemaRegistry::new(&url);
        let tb = TestApp::start(app(registry.clone())).await?;
        tb.assert_running();

        // The app's client shares its cache with every clone, this one included.
        let invoices = registry
            .cached_subject("invoices-value")
            .ok_or("startup did not register the invoices subject")?;
        assert_eq!(invoices.schema_type(), SchemaType::Json);
        let warmed = registry
            .cached_subject("payments-value")
            .ok_or("startup did not warm the payments subject")?;
        assert_eq!(warmed.id(), payments);
        Ok(())
    }
}
