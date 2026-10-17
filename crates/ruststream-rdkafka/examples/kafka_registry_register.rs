//! Registering a subject at startup, and warming one that another team owns.
//!
//! ```text
//! SCHEMA_REGISTRY_TEST_URL=http://127.0.0.1:8081 \
//!     cargo test --example kafka_registry_register --features schema-registry,testing
//! ```

use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::schema_registry::JsonSchema;
use ruststream_rdkafka::{KafkaError, SchemaFrame, SchemaRegistry};
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Payment {
    id: i64,
    amount: i64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing, JsonSchema)]
#[outgoing(name = "invoices")]
struct Invoice {
    payment: i64,
    amount: i64,
}

#[subscriber("payments", reply)]
async fn bill(payment: &Payment) -> Invoice {
    tracing::info!(payment.id, payment.amount, "invoice issued");
    Invoice {
        payment: payment.id,
        amount: payment.amount,
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
            b.include(bill);
        })
}

#[ruststream::app]
fn service() -> impl App {
    app(SchemaRegistry::new("http://localhost:8081"))
}
// --8<-- [end:app]

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
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
