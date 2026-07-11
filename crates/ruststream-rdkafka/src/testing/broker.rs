//! The in-process broker: core trait impls plus the `TestableBroker` registration.

use std::fmt;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{Broker, DescribeServer, OutgoingMessage, RawMessage, ServerSpec, Subscribe};

use super::publisher::KafkaTestPublisher;
use super::router::KeyRouter;
use super::subscriber::KafkaTestSubscriber;
use crate::error::KafkaError;

pub(crate) struct TestBrokerState {
    pub(crate) router: KeyRouter,
    coordinator: OnceLock<Coordinator>,
}

impl TestBrokerState {
    pub(crate) fn install(&self, coordinator: Coordinator) {
        // A second install on the same broker is ignored on purpose: the trait demands
        // idempotency.
        let _ = self.coordinator.set(coordinator);
    }

    pub(crate) fn coordinator(&self) -> Option<Coordinator> {
        self.coordinator.get().cloned()
    }
}

impl Default for TestBrokerState {
    fn default() -> Self {
        Self {
            router: KeyRouter::default(),
            coordinator: OnceLock::new(),
        }
    }
}

impl fmt::Debug for TestBrokerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestBrokerState")
            .field("router", &self.router)
            .finish_non_exhaustive()
    }
}

/// In-process broker for application tests: same descriptors, no Kafka cluster.
///
/// Clones share one router, so a publisher and a subscriber cloned from the same broker see
/// each other; separate [`new`](Self::new) calls are fully isolated.
///
/// # Examples
///
/// ```
/// use ruststream::{Broker, OutgoingMessage, Publisher, Subscriber};
/// use ruststream_rdkafka::testing::KafkaTestBroker;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), ruststream_rdkafka::KafkaError> {
/// let broker = KafkaTestBroker::new();
/// let mut subscriber = broker.subscribe("orders").await?;
/// broker.publisher().publish(OutgoingMessage::new("orders", b"{}")).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default)]
pub struct KafkaTestBroker {
    state: Arc<TestBrokerState>,
}

impl KafkaTestBroker {
    /// Creates an isolated in-process broker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Subscribes to `topic` (exact-name routing; no groups or partitions in-process).
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::InvalidOptions`] when `topic` is empty or a `^` pattern.
    // Async without an await on purpose: call-site parity with the real broker, so application
    // code and tests compile unchanged against either.
    #[allow(clippy::unused_async)]
    pub async fn subscribe(
        &self,
        topic: impl Into<String>,
    ) -> Result<KafkaTestSubscriber, KafkaError> {
        self.subscribe_topics(std::slice::from_ref(&topic.into()))
            .await
    }

    /// Subscribes to several topics as one subscription, mirroring
    /// [`KafkaTopic::and_topic`](crate::KafkaTopic::and_topic): every name routes exactly.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::InvalidOptions`] when a name is empty, or when a name is a `^`
    /// pattern: the in-process broker routes by exact topic name, so pattern subscriptions
    /// need a real cluster.
    // Async without an await on purpose: call-site parity with the real broker.
    #[allow(clippy::unused_async)]
    pub async fn subscribe_topics(
        &self,
        topics: &[String],
    ) -> Result<KafkaTestSubscriber, KafkaError> {
        for topic in topics {
            if topic.is_empty() {
                return Err(KafkaError::InvalidOptions(
                    "topic name must not be empty; subscribe with the topic the handler \
                     consumes"
                        .to_owned(),
                ));
            }
            if topic.starts_with('^') {
                return Err(KafkaError::InvalidOptions(format!(
                    "the in-process test broker routes by exact topic name; the pattern \
                     {topic:?} needs a real cluster",
                )));
            }
        }
        Ok(KafkaTestSubscriber::open_many(&self.state, topics))
    }

    /// A publisher into this broker's router.
    #[must_use]
    pub fn publisher(&self) -> KafkaTestPublisher {
        KafkaTestPublisher::new(Arc::clone(&self.state))
    }
}

impl Broker for KafkaTestBroker {
    type Error = KafkaError;

    async fn connect(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        self.state.router.clear();
        Ok(())
    }
}

// `Self::subscribe` inside this impl would resolve to the trait method and recurse; the type
// name is the only way to reach the inherent one.
#[allow(clippy::use_self)]
impl Subscribe for KafkaTestBroker {
    type Subscriber = KafkaTestSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        KafkaTestBroker::subscribe(self, name).await
    }
}

impl DescribeServer for KafkaTestBroker {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::in_process("kafka")
    }
}

// --8<-- [start:testable]
impl TestableBroker for KafkaTestBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        self.state.install(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        self.state.router.publish(
            message.name(),
            &Bytes::copy_from_slice(message.payload()),
            message.headers(),
            self.state.coordinator().as_ref(),
        );
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.state.router.published(name)
    }
}

ruststream::register_testable_broker!(KafkaTestBroker);
// --8<-- [end:testable]
