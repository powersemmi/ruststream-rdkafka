//! A service on a cluster that authenticates its clients: SASL over TLS, set through the broker's
//! `config` passthrough. TLS needs the `ssl` (system OpenSSL) or `ssl-vendored` feature.
//!
//! ```text
//! cargo test --example kafka_broker_security --features testing,ssl
//! ```

use std::env;

use serde::{Deserialize, Serialize};

use ruststream_rdkafka::prelude::*;

#[subscriber("orders")]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

// --8<-- [start:broker]
/// The cluster's SASL_SSL listener, with the service's own credentials.
fn broker(password: &str) -> KafkaBroker {
    KafkaBroker::new(["kafka-1.internal:9093", "kafka-2.internal:9093"])
        .default_group("orders-svc")
        .config("security.protocol", "SASL_SSL")
        .config("sasl.mechanism", "SCRAM-SHA-512")
        .config("sasl.username", "orders-svc")
        .config("sasl.password", password)
        .config("ssl.ca.location", "/etc/kafka/ca.pem")
}

#[ruststream::app]
fn app() -> impl App {
    let password = env::var("KAFKA_PASSWORD").unwrap_or_default();
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker(&password), |b| {
        b.include(place);
    })
}
// --8<-- [end:broker]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use super::{Order, app, place};

    #[tokio::test]
    async fn the_secured_service_handles_an_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KafkaBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test]
    async fn a_misspelled_protocol_fails_startup() {
        let broker = KafkaBroker::new(["kafka-1.internal:9093"])
            .default_group("orders-svc")
            .config("security.protocol", "SASL_TLS");
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
            b.include(place);
        });

        let refused = TestApp::start(app).await.err().map(|err| err.to_string());
        assert!(
            refused
                .as_deref()
                .is_some_and(|err| err.contains("security.protocol")),
            "{refused:?}"
        );
    }
}
