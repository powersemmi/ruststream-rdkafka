//! The crate error type shared by the broker, publishers, and subscribers.

use std::error::Error as StdError;

use thiserror::Error;

/// Errors returned by [`KafkaBroker`](crate::KafkaBroker) and the types it hands out.
///
/// Underlying [`rdkafka`](https://docs.rs/rdkafka) errors are boxed as sources so the client
/// library does not leak into this crate's public API surface.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum KafkaError {
    /// Creating a client failed or the cluster was unreachable during the connect probe.
    #[error("kafka connection error: {0}")]
    Connect(#[source] Box<dyn StdError + Send + Sync>),

    /// Publishing a message failed or the broker did not confirm its delivery.
    #[error("kafka publish error: {0}")]
    Publish(#[source] Box<dyn StdError + Send + Sync>),

    /// Creating a consumer or subscribing it to its topic failed.
    #[error("kafka subscribe error: {0}")]
    Subscribe(#[source] Box<dyn StdError + Send + Sync>),

    /// Receiving a delivery from an open consumer failed.
    #[error("kafka consume error: {0}")]
    Consume(#[source] Box<dyn StdError + Send + Sync>),

    /// An operation needed the live connection before `Broker::connect` resolved it.
    ///
    /// The runtime connects the broker once at startup; a publisher handed out earlier resolves
    /// the shared connection on first use. Seeing this error means the operation ran before
    /// `connect` completed.
    #[error("kafka broker is not connected; `Broker::connect` must complete first")]
    NotConnected,

    /// The requested combination of options cannot be executed.
    ///
    /// The message names the offending option and the remediation.
    #[error("invalid options: {0}")]
    InvalidOptions(String),

    /// `begin_transaction` found a transaction already open on this publisher.
    ///
    /// One producer runs one transaction at a time, so a second begin means two flows share
    /// one publisher; erroring beats silently merging their messages into one transaction.
    /// Concurrent transactional flows need distinct publishers - one per partition via
    /// [`TransactionalPartitions`](crate::TransactionalPartitions), or distinct explicit ids.
    #[error(
        "a transaction is already open on this publisher; one publisher runs one transaction \
         at a time - use distinct publishers (for example TransactionalPartitions) for \
         concurrent transactional flows"
    )]
    TransactionBusy,
}

impl KafkaError {
    pub(crate) fn connect(err: rdkafka::error::KafkaError) -> Self {
        Self::Connect(Box::new(err))
    }

    pub(crate) fn publish(err: rdkafka::error::KafkaError) -> Self {
        Self::Publish(Box::new(err))
    }

    pub(crate) fn subscribe(err: rdkafka::error::KafkaError) -> Self {
        Self::Subscribe(Box::new(err))
    }

    pub(crate) fn consume(err: rdkafka::error::KafkaError) -> Self {
        Self::Consume(Box::new(err))
    }
}
