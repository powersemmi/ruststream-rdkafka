//! The service of the production guide, written without the macros: an orders service
//! configured for a cluster that takes SASL over TLS, with its address and credentials read from
//! the environment.
//!
//! ```text
//! KAFKA_SERVERS=broker-1.example.com:9093 KAFKA_USERNAME=orders-svc KAFKA_PASSWORD=... \
//!     cargo run --example manual_guide_production --features ssl -- run
//! ```

use std::env;
use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:broker]
/// Where the cluster is and who the service is, as the deployment provides them.
struct Cluster {
    servers: String,
    username: String,
    password: String,
}

impl Cluster {
    fn from_env() -> Self {
        let read = |name: &str| env::var(name).unwrap_or_default();
        Self {
            servers: read("KAFKA_SERVERS"),
            username: read("KAFKA_USERNAME"),
            password: read("KAFKA_PASSWORD"),
        }
    }
}

fn broker(cluster: Cluster) -> KafkaBroker {
    KafkaBroker::new([cluster.servers])
        .default_group("orders-svc")
        .config("security.protocol", "SASL_SSL")
        .config("sasl.mechanism", "SCRAM-SHA-512")
        .config("sasl.username", cluster.username)
        .config("sasl.password", cluster.password)
        .producer_config("acks", "all")
        .producer_config("enable.idempotence", "true")
        .connect_timeout(Duration::from_secs(10))
        .flush_timeout(Duration::from_secs(20))
}
// --8<-- [end:broker]

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// What `#[derive(Outgoing)]` with `#[outgoing(name = "orders")]` writes.
impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

impl MessageInfo for Order {
    const NAME: &'static str = "Order";
    const DESCRIPTION: Option<&'static str> = None;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Confirmation {
    id: u64,
}

impl OutgoingDestination for Confirmation {
    type Form = FixedName;

    const DESTINATION: &'static str = "confirmations";
}

impl MessageHeaders for Confirmation {
    type Contract = NoHeaders;
}

impl MessageInfo for Confirmation {
    const NAME: &'static str = "Confirmation";
    const DESCRIPTION: Option<&'static str> = None;
}

// --8<-- [start:service]
struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        tracing::info!(order.id, "order confirmed");
        ready(Ok(Confirmation { id: order.id }))
    }
}

fn service(broker: KafkaBroker) -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(
            subscriber("orders", Confirm)
                .map_source(|_| KafkaTopic::new("orders").commit(Commit::Tracked))
                .reply()
                .build(),
        )
        .out_reply(Publish::default().queue_timeout(Duration::from_secs(5)));
    })
}

fn app() -> RustStream {
    service(broker(Cluster::from_env()))
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use super::{Cluster, Confirmation, Order, broker, service};

    fn cluster() -> Cluster {
        Cluster {
            servers: "broker-1.example.com:9093".to_owned(),
            username: "orders-svc".to_owned(),
            password: "s3cret-pa55".to_owned(),
        }
    }

    #[tokio::test]
    async fn the_production_configuration_runs_in_process() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker(cluster()))).await?;

        tb.broker::<KafkaBroker>()
            .message(&Order { id: 1 })
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .published::<Confirmation>("confirmations")
            .assert_called_once()
            .with(&Confirmation { id: 1 });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_property_librdkafka_refuses_fails_the_start() {
        let refused = broker(cluster()).config("security.protocol", "SASL_TLS");

        let error = TestApp::start(service(refused))
            .await
            .err()
            .map(|error| error.to_string());

        assert!(
            error
                .as_deref()
                .is_some_and(|error| error.contains("security.protocol")),
            "{error:?}",
        );
    }
}
