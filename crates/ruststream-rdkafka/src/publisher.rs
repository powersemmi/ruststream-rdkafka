//! The publisher: fire-and-confirm production onto Kafka topics.

use std::time::Duration;

use rdkafka::producer::FutureRecord;
use rdkafka::util::Timeout;
use ruststream::{OutgoingMessage, Publisher};

use crate::broker::SharedConn;
use crate::convert;
use crate::error::KafkaError;

/// A producer handle sharing the broker's connection.
///
/// [`OutgoingMessage::name`] is the destination topic. A
/// [`PARTITION_KEY_HEADER`](crate::PARTITION_KEY_HEADER) header becomes the record's native key,
/// so Kafka routes messages that share a key to the same partition; without it the configured
/// partitioner picks one.
///
/// Each publish awaits the broker's delivery report, so an `Ok` means the cluster accepted the
/// record (durability then depends on the producer's `acks` setting, configurable through
/// [`KafkaBroker::producer_config`](crate::KafkaBroker::producer_config)).
///
/// Obtained from [`KafkaBroker::publisher`](crate::KafkaBroker::publisher); usable before
/// `Broker::connect` resolves the connection (publishing earlier returns
/// [`KafkaError::NotConnected`]).
#[derive(Debug, Clone)]
pub struct KafkaPublisher {
    conn: SharedConn,
    queue_timeout: Option<Duration>,
}

impl KafkaPublisher {
    pub(crate) fn new(conn: SharedConn) -> Self {
        Self {
            conn,
            queue_timeout: None,
        }
    }

    /// How long a publish may wait for space when librdkafka's local queue is full, before
    /// failing with a queue-full error. Without it a publish waits for space indefinitely,
    /// which is the natural back-pressure behavior.
    #[must_use]
    pub fn queue_timeout(mut self, timeout: Duration) -> Self {
        self.queue_timeout = Some(timeout);
        self
    }
}

impl Publisher for KafkaPublisher {
    type Error = KafkaError;

    /// Publishes `msg` to the topic named by [`OutgoingMessage::name`] and awaits the delivery
    /// report.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::NotConnected`] before `Broker::connect` resolves the connection and
    /// [`KafkaError::Publish`] when the cluster rejects the record or the delivery times out
    /// (librdkafka's `message.timeout.ms`).
    ///
    /// # Cancel safety
    ///
    /// Not cancel safe: dropping the future may leave the record in flight, delivered or not.
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let state = self.conn.get().ok_or(KafkaError::NotConnected)?;
        let (headers, key) = convert::headers_for_publish(msg.headers());

        let mut record = FutureRecord::<[u8], [u8]>::to(msg.name()).payload(msg.payload());
        if let Some(key) = &key {
            record = record.key(key.as_ref());
        }
        if let Some(headers) = headers {
            record = record.headers(headers);
        }

        let queue_timeout = self.queue_timeout.map_or(Timeout::Never, Timeout::After);
        state
            .producer()
            .send(record, queue_timeout)
            .await
            .map(|_delivery| ())
            .map_err(|(err, _record)| KafkaError::publish(err))
    }
}
