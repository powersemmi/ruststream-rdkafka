//! The in-process publisher.

use std::future::{Future, ready};
use std::sync::Arc;

use bytes::Bytes;
use ruststream::{DefaultPublish, OutgoingMessage, PairError, PublishPolicy, Publisher};

use super::broker::{ConnectedKafkaTestBroker, TestBrokerState};
use crate::error::KafkaError;
use crate::publisher::KafkaPublish;

/// Publisher into the in-process router.
///
/// Mirrors [`KafkaPublisher`](crate::KafkaPublisher) delivery semantics minus the cluster: the
/// message name is the topic, and the partition-key header rides along for keyed worker lanes.
#[derive(Debug, Clone)]
pub struct KafkaTestPublisher {
    state: Arc<TestBrokerState>,
}

impl KafkaTestPublisher {
    pub(crate) fn new(state: Arc<TestBrokerState>) -> Self {
        Self { state }
    }
}

/// The in-process broker pairs the real [`KafkaPublish`] policy, so an application's include
/// sites and routers compile unchanged against either broker.
impl PublishPolicy<ConnectedKafkaTestBroker> for KafkaPublish {
    type Live = KafkaTestPublisher;

    fn pair(
        self,
        connected: &ConnectedKafkaTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher(self)))
    }
}

impl DefaultPublish for ConnectedKafkaTestBroker {
    type Policy = KafkaPublish;
}

impl Publisher for KafkaTestPublisher {
    type Error = KafkaError;

    /// Routes `msg` to subscribers of the topic named by `msg.name()`.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::InvalidOptions`] when the topic name is empty, and
    /// [`KafkaError::Closed`] once the transport this handle aliases has been shut down.
    fn publish(&self, msg: OutgoingMessage<'_>) -> impl Future<Output = Result<(), Self::Error>> {
        if msg.name().is_empty() {
            return ready(Err(KafkaError::InvalidOptions(
                "topic name must not be empty; the outgoing message name is the destination \
                 topic"
                    .to_owned(),
            )));
        }
        if let Err(err) = self.state.ensure_open(msg.name()) {
            return ready(Err(err));
        }
        self.state.router.publish(
            msg.name(),
            &Bytes::copy_from_slice(msg.payload()),
            msg.headers(),
            self.state.coordinator().as_ref(),
        );
        ready(Ok(()))
    }
}
