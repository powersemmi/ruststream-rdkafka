//! The in-process broker ladder: core trait impls plus the `TestableBroker` registration.

use std::fmt;
use std::future::{Future, ready};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{
    Broker, ConnectedBroker, DescribeServer, OutgoingMessage, RawMessage, ServerSpec, Subscribe,
};

use super::publisher::KafkaTestPublisher;
use super::router::KeyRouter;
use super::subscriber::KafkaTestSubscriber;
use crate::error::KafkaError;

pub(crate) struct TestBrokerState {
    pub(crate) router: KeyRouter,
    /// Mirrors the real broker's post-shutdown behaviour: a publisher aliasing a shut-down
    /// transport must report an error rather than route into a dead router.
    closed: AtomicBool,
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

    pub(crate) fn ensure_open(&self, topic: &str) -> Result<(), KafkaError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(KafkaError::Closed {
                topic: topic.to_owned(),
            });
        }
        Ok(())
    }
}

impl Default for TestBrokerState {
    fn default() -> Self {
        Self {
            router: KeyRouter::default(),
            closed: AtomicBool::new(false),
            coordinator: OnceLock::new(),
        }
    }
}

impl fmt::Debug for TestBrokerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestBrokerState")
            .field("router", &self.router)
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// In-process broker for application tests: same descriptors, no Kafka cluster.
///
/// The unconnected form, mirroring [`KafkaBroker`](crate::KafkaBroker): construction is
/// synchronous and `connect` yields the [`ConnectedKafkaTestBroker`] everything else hangs off.
///
/// # Examples
///
/// ```
/// use ruststream::{Broker, OutgoingMessage, Publisher, Subscriber};
/// use ruststream_rdkafka::KafkaPublish;
/// use ruststream_rdkafka::testing::KafkaTestBroker;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), ruststream_rdkafka::KafkaError> {
/// let broker = KafkaTestBroker::new().connect().await?;
/// let mut subscriber = broker.subscribe_with("orders").await?;
/// broker
///     .publisher(KafkaPublish::default())
///     .publish(OutgoingMessage::new("orders", b"{}"))
///     .await?;
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
}

impl Broker for KafkaTestBroker {
    type Error = KafkaError;
    type Connected = ConnectedKafkaTestBroker;

    /// Connecting an in-process transport is free; the ladder shape is what matters.
    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        ready(Ok(ConnectedKafkaTestBroker { state: self.state }))
    }
}

impl DescribeServer for KafkaTestBroker {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::in_process("kafka")
    }
}

/// The connected form of [`KafkaTestBroker`].
///
/// Clones share one router, so a publisher and a subscriber from the same broker see each
/// other; separate [`KafkaTestBroker::new`] calls are fully isolated.
#[derive(Debug, Clone, Default)]
pub struct ConnectedKafkaTestBroker {
    state: Arc<TestBrokerState>,
}

impl ConnectedKafkaTestBroker {
    /// Subscribes to `topic` (exact-name routing; no groups or partitions in-process).
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::InvalidOptions`] when `topic` is empty or a `^` pattern.
    // Returns a future without awaiting on purpose: call-site parity with the real broker, so
    // application code and tests compile unchanged against either.
    pub fn subscribe_with(
        &self,
        topic: impl Into<String>,
    ) -> impl Future<Output = Result<KafkaTestSubscriber, KafkaError>> {
        let topics = [topic.into()];
        ready(self.open_subscription(&topics))
    }

    /// Subscribes to several topics as one subscription, mirroring
    /// [`KafkaTopic::and_topic`](crate::KafkaTopic::and_topic): every name routes exactly.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::InvalidOptions`] when a name is empty, or when a name is a `^`
    /// pattern: the in-process broker routes by exact topic name, so pattern subscriptions
    /// need a real cluster.
    pub fn subscribe_topics(
        &self,
        topics: &[String],
    ) -> impl Future<Output = Result<KafkaTestSubscriber, KafkaError>> {
        ready(self.open_subscription(topics))
    }

    /// The synchronous body behind both subscribe entry points, kept apart so the validation
    /// errors stay `?` rather than a chain of early `ready(Err(..))` returns.
    fn open_subscription(&self, topics: &[String]) -> Result<KafkaTestSubscriber, KafkaError> {
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
            self.state.ensure_open(topic)?;
        }
        Ok(KafkaTestSubscriber::open_many(&self.state, topics))
    }

    /// A publisher into this broker's router.
    ///
    /// Takes the same [`KafkaPublish`](crate::KafkaPublish) policy the real broker does, so an
    /// application's include sites compile unchanged against either. The policy's options
    /// describe librdkafka's local queue, which the in-process router does not have, so they
    /// carry no behaviour here.
    #[must_use]
    pub fn publisher(&self, _policy: crate::KafkaPublish) -> KafkaTestPublisher {
        KafkaTestPublisher::new(Arc::clone(&self.state))
    }
}

impl ConnectedBroker for ConnectedKafkaTestBroker {
    type Error = KafkaError;
    type Closed = ();

    /// Drops every subscription and marks the transport closed, so publishers aliasing it
    /// error afterwards exactly as they do against a real cluster.
    fn shutdown(self) -> impl Future<Output = Result<Self::Closed, Self::Error>> {
        self.state.closed.store(true, Ordering::Release);
        self.state.router.clear();
        ready(Ok(()))
    }
}

impl Subscribe for ConnectedKafkaTestBroker {
    type Subscriber = KafkaTestSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.subscribe_with(name).await
    }
}

// --8<-- [start:testable]
// The harness drives the connected form: TestApp connects every registered broker before it
// recovers the in-process transport, and run_suite scenarios receive connected brokers.
impl TestableBroker for ConnectedKafkaTestBroker {
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

ruststream::register_testable_broker!(ConnectedKafkaTestBroker);
// --8<-- [end:testable]
