//! Topics, partitions and the records they retain.
//!
//! A partition is an append-only log and a record's offset is its index in it. Offsets are never
//! reused, and a transaction's commit or abort takes an offset of its own for the control marker
//! a consumer never sees, which is why a consumer's offsets can have gaps here as on a cluster.

use std::sync::Arc;

use bytes::Bytes;

use crate::convert;

/// One wire header of a stored record: a name and a value, which Kafka lets be null.
pub(crate) type WireHeader = (String, Option<Bytes>);

/// Who produced a record inside a transaction: the transactional id and the producer epoch the
/// pairing was fenced with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProducerId {
    pub(crate) transactional_id: String,
    pub(crate) epoch: u64,
}

/// Whether a data record is visible, and to which readers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Visibility {
    /// Produced outside a transaction, or by one that committed.
    Committed,
    /// Produced inside a transaction that is still open: a `read_committed` reader stops in front
    /// of it, a `read_uncommitted` one reads it.
    Pending(ProducerId),
    /// Produced inside a transaction that aborted: only a `read_uncommitted` reader sees it.
    Aborted,
}

/// What occupies one offset.
#[derive(Debug, Clone)]
pub(crate) enum Entry {
    /// A record a producer wrote.
    Data(Arc<Stored>, Visibility),
    /// A transaction marker: it takes an offset and is delivered to nobody.
    Control,
}

/// A record as the log retains it.
#[derive(Debug)]
pub(crate) struct Stored {
    /// Cluster-wide publish order, so the publish log reads back in the order records were
    /// produced across a topic's partitions.
    pub(crate) sequence: u64,
    /// `CreateTime`, in milliseconds since the epoch, stamped when the record was produced.
    pub(crate) timestamp: i64,
    pub(crate) payload: Bytes,
    pub(crate) key: Option<Bytes>,
    pub(crate) headers: Vec<WireHeader>,
}

/// How a reader treats records of transactions (librdkafka's `isolation.level`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Isolation {
    /// Stops at the first record of an open transaction and skips aborted ones.
    ReadCommitted,
    /// Reads every data record as it is written.
    ReadUncommitted,
}

impl Isolation {
    /// The next offset at or after `from` this reader is handed a record from, with that record,
    /// or `None` when the reader has to wait.
    pub(crate) fn next(self, entries: &[Entry], from: i64) -> Option<(i64, &Arc<Stored>)> {
        let start = usize::try_from(from).unwrap_or(0);
        for (index, entry) in entries.iter().enumerate().skip(start) {
            match entry {
                Entry::Data(stored, Visibility::Committed) => {
                    return Some((offset_of(index), stored));
                }
                Entry::Data(stored, Visibility::Pending(_) | Visibility::Aborted)
                    if self == Self::ReadUncommitted =>
                {
                    return Some((offset_of(index), stored));
                }
                // The last stable offset: a committed reader reads nothing past an open
                // transaction until it resolves.
                Entry::Data(_, Visibility::Pending(_)) => return None,
                Entry::Control | Entry::Data(_, Visibility::Aborted) => {}
            }
        }
        None
    }

    /// How many records a reader at `from` is still to be handed right now.
    pub(crate) fn backlog(self, entries: &[Entry], from: i64) -> usize {
        let mut count = 0;
        let mut at = from;
        while let Some((offset, _)) = self.next(entries, at) {
            count += 1;
            at = offset + 1;
        }
        count
    }
}

/// A log index as the offset it is.
pub(crate) fn offset_of(index: usize) -> i64 {
    i64::try_from(index).unwrap_or(i64::MAX)
}

/// One partition's log.
#[derive(Debug, Default)]
pub(crate) struct Partition {
    pub(crate) entries: Vec<Entry>,
}

impl Partition {
    /// The high watermark: the offset the next record takes.
    pub(crate) fn end(&self) -> i64 {
        offset_of(self.entries.len())
    }

    /// The offset of the first record stamped at or after `when`, or the end of the log.
    pub(crate) fn offset_for_time(&self, when: i64) -> i64 {
        self.entries
            .iter()
            .position(|entry| matches!(entry, Entry::Data(stored, _) if stored.timestamp >= when))
            .map_or_else(|| self.end(), offset_of)
    }
}

/// A topic: its partitions, and the cursor of its keyless placement.
#[derive(Debug)]
pub(crate) struct Topic {
    pub(crate) partitions: Vec<Partition>,
    keyless: usize,
}

/// How many partitions a topic has when it comes into being: the broker default
/// (`num.partitions`), which the test stand runs with too.
pub(crate) const DEFAULT_PARTITIONS: usize = 1;

impl Topic {
    pub(crate) fn new(partitions: usize) -> Self {
        Self {
            partitions: (0..partitions.max(1))
                .map(|_| Partition::default())
                .collect(),
            keyless: 0,
        }
    }

    /// The partition a record lands on: the one named, the one its key hashes to
    /// (librdkafka's default `consistent_random` partitioner, a CRC-32 of the key), or the next
    /// one in turn for a record with neither.
    pub(crate) fn place(&mut self, named: Option<i32>, key: Option<&[u8]>) -> Option<usize> {
        let count = self.partitions.len();
        match (named, key) {
            (Some(partition), _) => usize::try_from(partition)
                .ok()
                .filter(|index| *index < count),
            (None, Some(key)) if !key.is_empty() => {
                let hash = usize::try_from(crc32(key)).unwrap_or(0);
                Some(hash % count)
            }
            (None, _) => {
                let index = self.keyless % count;
                self.keyless = self.keyless.wrapping_add(1);
                Some(index)
            }
        }
    }
}

/// Whether Kafka accepts `name` as a topic: 1 to 249 characters from `[a-zA-Z0-9._-]`, and not
/// `.` or `..`.
pub(crate) fn legal_topic(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 249
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// The wire headers a publish's header map becomes: every header except the ones this crate maps
/// onto the record itself (the key) or strips (the exactly-once source coordinates).
pub(crate) fn wire_headers(headers: &ruststream::HeaderMap) -> Vec<WireHeader> {
    headers
        .iter()
        .filter(|(name, _)| convert::rides_the_wire(name))
        .map(|(name, value)| (name.to_owned(), Some(Bytes::copy_from_slice(value))))
        .collect()
}

/// The CRC-32 librdkafka's consistent partitioner hashes a key with (the IEEE polynomial, as in
/// zlib).
fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data(visibility: Visibility) -> Entry {
        Entry::Data(
            Arc::new(Stored {
                sequence: 0,
                timestamp: 0,
                payload: Bytes::new(),
                key: None,
                headers: Vec::new(),
            }),
            visibility,
        )
    }

    fn pending() -> Visibility {
        Visibility::Pending(ProducerId {
            transactional_id: "tx".to_owned(),
            epoch: 1,
        })
    }

    #[test]
    fn the_crc_is_the_ieee_one() {
        // The check value of CRC-32/ISO-HDLC, the one zlib and librdkafka compute.
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn a_committed_reader_stops_at_an_open_transaction_and_skips_an_aborted_one() {
        let entries = vec![
            data(Visibility::Committed),
            data(Visibility::Aborted),
            Entry::Control,
            data(Visibility::Committed),
            data(pending()),
            data(Visibility::Committed),
        ];
        let committed = Isolation::ReadCommitted;
        assert_eq!(committed.next(&entries, 0).map(|(at, _)| at), Some(0));
        assert_eq!(committed.next(&entries, 1).map(|(at, _)| at), Some(3));
        assert_eq!(committed.next(&entries, 4).map(|(at, _)| at), None);
        assert_eq!(committed.backlog(&entries, 0), 2);

        let uncommitted = Isolation::ReadUncommitted;
        assert_eq!(uncommitted.next(&entries, 1).map(|(at, _)| at), Some(1));
        assert_eq!(uncommitted.backlog(&entries, 0), 5);
    }

    #[test]
    fn a_key_always_lands_on_one_partition_and_a_named_partition_must_exist() {
        let mut topic = Topic::new(4);
        let first = topic.place(None, Some(b"tenant-1"));
        assert_eq!(first, topic.place(None, Some(b"tenant-1")));
        assert_eq!(topic.place(Some(3), Some(b"tenant-1")), Some(3));
        assert_eq!(topic.place(Some(4), None), None);
        assert_eq!(topic.place(Some(-1), None), None);
        let keyless: Vec<_> = (0..4).map(|_| topic.place(None, None)).collect();
        assert_eq!(keyless, [Some(0), Some(1), Some(2), Some(3)]);
    }

    #[test]
    fn topic_names_follow_kafkas_rule() {
        assert!(legal_topic("orders.eu-1_v2"));
        for bad in ["", ".", "..", "orders eu", "orders/eu", &"x".repeat(250)] {
            assert!(!legal_topic(bad), "{bad:?} must be refused");
        }
    }
}
