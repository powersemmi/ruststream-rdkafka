//! The service of `examples/kafka_publishing_publish.rs` written without `#[subscriber]`: the
//! reply body implements `Handle`, and the mount site pairs the same publish policy.
//!
//! ```text
//! cargo test --example manual_kafka_publishing_publish --features testing
//! cargo run --example manual_kafka_publishing_publish -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "confirmations")]
struct Confirmation {
    order_id: u64,
}

struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        tracing::info!(order.id, "order confirmed");
        ready(Ok(Confirmation { order_id: order.id }))
    }
}

fn orders() -> KafkaTopic {
    KafkaTopic::new("orders").group("orders-svc")
}

// --8<-- [start:publish]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]),
        |b| {
            let confirm = subscriber("orders", Confirm).map_source(|_| orders());
            // A full local queue fails the reply after five seconds instead of holding the
            // handler until librdkafka has room again.
            b.include(confirm.reply().build())
                .out_reply(Publish::default().queue_timeout(Duration::from_secs(5)));
        },
    )
}
// --8<-- [end:publish]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use super::{Confirmation, Order, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_reply_lands_on_the_topic_its_type_names() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KafkaBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .published::<Confirmation>("confirmations")
            .assert_called_once()
            .with(&Confirmation { order_id: 7 })
            .assert_options_default();
        tb.shutdown().await?;
        Ok(())
    }
}
