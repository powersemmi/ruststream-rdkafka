//! The service of `examples/kafka_broker_security.rs` without the macros: the handler is a named type
//! and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_kafka_broker_security --features testing,ssl
//! ```

use std::env;
use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_rdkafka::prelude::*;

struct Place;

impl Handle<Order> for Place {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order placed");
        ready(Ok(()))
    }
}

// --8<-- [start:broker]
/// The cluster's `SASL_SSL` listener, with the service's own credentials.
fn broker(password: &str) -> KafkaBroker {
    KafkaBroker::new(["kafka-1.internal:9093", "kafka-2.internal:9093"])
        .default_group("orders-svc")
        .config("security.protocol", "SASL_SSL")
        .config("sasl.mechanism", "SCRAM-SHA-512")
        .config("sasl.username", "orders-svc")
        .config("sasl.password", password)
        .config("ssl.ca.location", "/etc/kafka/ca.pem")
}

fn app() -> impl App {
    let password = env::var("KAFKA_PASSWORD").unwrap_or_default();
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker(&password), |b| {
        b.include(subscriber("orders", Place).build());
    })
}
// --8<-- [end:broker]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: the type names the topic it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use super::{Order, Place, app};

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
            b.include(subscriber("orders", Place).build());
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
