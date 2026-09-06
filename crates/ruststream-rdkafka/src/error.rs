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

    /// A [`KafkaRetryPublisher`](crate::KafkaRetryPublisher) was used before its broker
    /// connected.
    ///
    /// Only the early publisher can report this: it is the one handle minted before
    /// [`Broker::connect`](ruststream::Broker::connect), for builder-time wiring that needs a
    /// live publisher (`retry_via`). Everything on the policy path pairs with the connected
    /// broker, so "not connected" is not representable there.
    #[error("kafka broker is not connected yet; cannot reach {topic}")]
    NotConnected {
        /// The topic the operation targeted.
        topic: String,
    },

    /// A handle aliasing the connection was used after the broker shut down.
    ///
    /// The lifecycle ladder makes misuse through the owner's handle a compile error:
    /// [`ConnectedBroker::shutdown`](ruststream::ConnectedBroker::shutdown) consumes the
    /// connected broker. Publishers paired off it earlier, and subscriptions still open, keep
    /// aliasing the closed connection, so their operations report this instead of silently
    /// succeeding against a dead connection.
    #[error("kafka connection is closed; cannot reach {topic}")]
    Closed {
        /// The topic the operation targeted, or the transactional id of a transaction control
        /// call.
        topic: String,
    },

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
        "a transaction is already open on publisher {id}; one publisher runs one transaction \
         at a time - use distinct publishers (for example the per-partition set) for \
         concurrent transactional flows"
    )]
    TransactionBusy {
        /// The transactional id of the publisher that already has an open transaction.
        id: String,
    },

    /// `commit` or `abort` was called with no transaction open on this publisher.
    #[error("no transaction is open on publisher {id}; `begin_transaction` opens one")]
    NoTransaction {
        /// The transactional id of the publisher the call was made on.
        id: String,
    },

    /// A Schema Registry request failed: unreachable registry, rejected credentials, an
    /// unknown schema id or subject, or a schema the registry refused.
    #[cfg(feature = "schema-registry")]
    #[error("schema registry error: {0}")]
    SchemaRegistry(#[source] Box<dyn StdError + Send + Sync>),

    /// A value could not be written to, or read from, its binary wire form.
    ///
    /// This is the byte lanes' own failure: the Avro or Protobuf encoder rejected the value,
    /// the payload is not the datum a schema describes, or a delivery does not carry the
    /// Confluent envelope the reader expected. It stays distinct from
    /// [`SchemaRegistry`](Self::SchemaRegistry), which is the registry conversation failing -
    /// the fixed-schema lane reaches no registry at all, so folding the two would make every
    /// diagnostic point at a component that was never involved.
    #[cfg(feature = "schema-registry")]
    #[error("wire format error: {0}")]
    WireFormat(#[source] Box<dyn StdError + Send + Sync>),
}

impl KafkaError {
    pub(crate) fn connect(err: rdkafka::error::KafkaError) -> Self {
        Self::Connect(Box::new(err))
    }

    pub(crate) fn publish(err: rdkafka::error::KafkaError) -> Self {
        Self::Publish(Box::new(err))
    }

    #[cfg(feature = "schema-registry")]
    pub(crate) fn schema_registry(err: impl StdError + Send + Sync + 'static) -> Self {
        Self::SchemaRegistry(Box::new(err))
    }

    // Only a format's own encoder produces one of these; the envelope's own failures come
    // through `malformed`, which the frame types report with no format feature enabled.
    #[cfg(any(feature = "avro", feature = "protobuf"))]
    pub(crate) fn wire_format(err: impl StdError + Send + Sync + 'static) -> Self {
        Self::WireFormat(Box::new(err))
    }

    /// The wire-format failure the lanes report for a payload that is structurally wrong (not
    /// framed, indexes truncated) rather than for another error's failure.
    #[cfg(feature = "schema-registry")]
    pub(crate) fn malformed(message: impl Into<String>) -> Self {
        Self::WireFormat(message.into().into())
    }

    pub(crate) fn subscribe(err: rdkafka::error::KafkaError) -> Self {
        Self::Subscribe(Box::new(err))
    }

    pub(crate) fn consume(err: rdkafka::error::KafkaError) -> Self {
        Self::Consume(Box::new(err))
    }
}
