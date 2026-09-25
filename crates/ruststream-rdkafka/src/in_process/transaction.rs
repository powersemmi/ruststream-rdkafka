//! Transactions: records held pending, fencing by epoch, the broker's timeout, and the offsets a
//! transaction commits into their groups.

use std::collections::BTreeSet;
use std::time::Duration;

use ruststream::Str;
use tokio::time::Instant;

use super::log::{Entry, ProducerId, Visibility};
use super::{Cluster, State};
use crate::error::KafkaError;

/// A transactional id's place in the cluster: the epoch its latest pairing holds, and the
/// transaction that pairing has open.
#[derive(Default)]
pub(super) struct Transactional {
    pub(super) epoch: u64,
    pub(super) open: Option<OpenTransaction>,
}

pub(super) struct OpenTransaction {
    /// The producer the transaction belongs to: its records are the ones the end resolves.
    pub(super) producer: ProducerId,
    pub(super) started: Instant,
    /// The partitions the transaction wrote to, which its marker goes to.
    pub(super) partitions: BTreeSet<(String, usize)>,
    /// Offsets to commit into their groups with the transaction.
    pub(super) offsets: Vec<(String, (String, i32), i64)>,
}

/// How a transaction ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Outcome {
    Commit,
    Abort,
}

impl Cluster {
    /// Pairs a transactional producer: the id's epoch moves on, which fences every earlier
    /// pairing, and a transaction an earlier one left open is aborted.
    pub(crate) fn init_transactions(&self, transactional_id: &str) -> ProducerId {
        let mut state = self.lock();
        let entry = state
            .transactions
            .entry(transactional_id.to_owned())
            .or_default();
        entry.epoch += 1;
        let epoch = entry.epoch;
        let left_open = entry.open.take();
        if let Some(open) = left_open {
            state.finish(open, Outcome::Abort);
        }
        let changes = state.reconcile();
        drop(state);
        self.apply(changes);
        ProducerId {
            transactional_id: transactional_id.to_owned(),
            epoch,
        }
    }

    /// Opens a transaction for `producer`.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Publish`] when a later pairing fenced the producer.
    pub(crate) fn begin(&self, producer: &ProducerId) -> Result<(), KafkaError> {
        let mut state = self.lock();
        state.expire(self.settings.transaction_timeout);
        state.check_epoch(producer)?;
        let entry = state
            .transactions
            .get_mut(&producer.transactional_id)
            .expect("a paired producer has an entry");
        entry.open = Some(OpenTransaction {
            producer: producer.clone(),
            started: Instant::now(),
            partitions: BTreeSet::new(),
            offsets: Vec::new(),
        });
        drop(state);
        // The broker aborts a transaction that outlives its timeout on its own clock; a timer is
        // what reaches a reader waiting in front of it.
        let cluster = self.this.clone();
        let timeout = self.settings.transaction_timeout;
        tokio::spawn(async move {
            tokio::time::sleep(timeout).await;
            if let Some(cluster) = cluster.upgrade() {
                let mut state = cluster.lock();
                state.expire(timeout);
                let changes = state.reconcile();
                drop(state);
                cluster.apply(changes);
            }
        });
        Ok(())
    }

    /// Adds consumed offsets to `producer`'s open transaction, to commit into `group` with it.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Publish`] when the producer was fenced or has no transaction open.
    pub(crate) fn send_offsets(
        &self,
        producer: &ProducerId,
        group: &str,
        positions: &[((Str, i32), i64)],
    ) -> Result<(), KafkaError> {
        let mut state = self.lock();
        state.expire(self.settings.transaction_timeout);
        state.check_epoch(producer)?;
        let Some(open) = state.open_transaction(producer) else {
            return Err(no_open_transaction(producer));
        };
        for ((topic, partition), next) in positions {
            open.offsets
                .push((group.to_owned(), (topic.to_string(), *partition), *next));
        }
        drop(state);
        Ok(())
    }

    /// Ends `producer`'s open transaction.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Publish`] when the producer was fenced, or when the transaction it
    /// would commit is no longer open (the cluster aborted it on its timeout).
    fn end(&self, producer: &ProducerId, outcome: Outcome) -> Result<(), KafkaError> {
        let mut state = self.lock();
        state.expire(self.settings.transaction_timeout);
        let result = state.check_epoch(producer).and_then(|()| {
            let open = state
                .transactions
                .get_mut(&producer.transactional_id)
                .and_then(|entry| entry.open.take());
            match open {
                Some(open) => {
                    state.finish(open, outcome);
                    Ok(())
                }
                None if outcome == Outcome::Abort => Ok(()),
                None => Err(no_open_transaction(producer)),
            }
        });
        let changes = state.reconcile();
        drop(state);
        self.apply(changes);
        result
    }

    /// Commits `producer`'s open transaction.
    ///
    /// # Errors
    ///
    /// See [`end`](Self::end).
    pub(crate) fn commit(&self, producer: &ProducerId) -> Result<(), KafkaError> {
        self.end(producer, Outcome::Commit)
    }

    /// Aborts `producer`'s open transaction.
    ///
    /// # Errors
    ///
    /// See [`end`](Self::end).
    pub(crate) fn abort(&self, producer: &ProducerId) -> Result<(), KafkaError> {
        self.end(producer, Outcome::Abort)
    }
}

impl State {
    /// Whether `producer` still holds its transactional id's epoch.
    pub(super) fn check_epoch(&self, producer: &ProducerId) -> Result<(), KafkaError> {
        let current = self
            .transactions
            .get(&producer.transactional_id)
            .map_or(0, |entry| entry.epoch);
        if current == producer.epoch {
            Ok(())
        } else {
            Err(KafkaError::Publish(
                format!(
                    "Broker: Producer fenced: transactional id {} was paired again, and this \
                     producer's epoch is no longer the current one",
                    producer.transactional_id
                )
                .into(),
            ))
        }
    }

    pub(super) fn open_transaction(
        &mut self,
        producer: &ProducerId,
    ) -> Option<&mut OpenTransaction> {
        self.transactions
            .get_mut(&producer.transactional_id)
            .filter(|entry| entry.epoch == producer.epoch)
            .and_then(|entry| entry.open.as_mut())
    }

    /// Aborts every transaction open longer than `timeout`, as the broker does.
    pub(super) fn expire(&mut self, timeout: Duration) {
        let now = Instant::now();
        let expired: Vec<OpenTransaction> = self
            .transactions
            .values_mut()
            .filter(|entry| {
                entry
                    .open
                    .as_ref()
                    .is_some_and(|open| now.duration_since(open.started) >= timeout)
            })
            .filter_map(|entry| entry.open.take())
            .collect();
        for open in expired {
            self.finish(open, Outcome::Abort);
        }
    }

    /// Resolves a transaction's records and writes its markers; a commit also commits the offsets
    /// it carries.
    pub(super) fn finish(&mut self, open: OpenTransaction, outcome: Outcome) {
        for (topic, index) in &open.partitions {
            let Some(partition) = self
                .topics
                .get_mut(topic)
                .and_then(|log| log.partitions.get_mut(*index))
            else {
                continue;
            };
            for entry in &mut partition.entries {
                if let Entry::Data(_, visibility @ Visibility::Pending(_)) = entry
                    && *visibility == Visibility::Pending(open.producer.clone())
                {
                    *visibility = match outcome {
                        Outcome::Commit => Visibility::Committed,
                        Outcome::Abort => Visibility::Aborted,
                    };
                }
            }
            partition.entries.push(Entry::Control);
        }
        if outcome == Outcome::Commit {
            for (group, key, next) in open.offsets {
                self.committed.entry(group).or_default().insert(key, next);
            }
        }
    }
}

fn no_open_transaction(producer: &ProducerId) -> KafkaError {
    KafkaError::Publish(
        format!(
            "Local: Erroneous state: producer {} has no transaction open",
            producer.transactional_id
        )
        .into(),
    )
}
