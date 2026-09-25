//! Consumer groups: who reads which partition, and where a partition resumes.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use ruststream::Str;

use super::{Failure, MemberSpec, Partition, Reader, State, partition_number};
use crate::subscription::StartOffset;

impl State {
    /// Whether `spec` shares an assignment strategy with every member already in `group`.
    pub(super) fn agrees_on_protocol(&self, group: &str, spec: &MemberSpec) -> bool {
        self.members
            .values()
            .filter(|member| {
                member.spec.group.as_deref() == Some(group) && member.failure.is_none()
            })
            .all(|member| {
                member
                    .spec
                    .strategies
                    .iter()
                    .any(|strategy| spec.strategies.contains(strategy))
            })
    }

    /// Hands the group's partitions out again, as a join or a leave does on a cluster.
    ///
    /// Every member's partitions are revoked and reassigned: librdkafka's default strategies are
    /// eager. A revoked partition's bookkeeping moves on to a fresh read position, and a partition
    /// resumes from the group's committed offset, whoever gets it.
    pub(super) fn rebalance(&mut self, group: &str) {
        let ids: Vec<u64> = self
            .members
            .iter()
            .filter(|(_, member)| {
                member.spec.group.as_deref() == Some(group)
                    && matches!(member.spec.reader, Reader::Subscribed { .. })
                    && !matches!(member.failure, Some(Failure::InconsistentProtocol))
            })
            .map(|(id, _)| *id)
            .collect();
        let Some(first) = ids.first() else {
            return;
        };
        let strategy = self.members[first]
            .spec
            .strategies
            .iter()
            .find(|strategy| {
                ids.iter()
                    .all(|id| self.members[id].spec.strategies.contains(strategy))
            })
            .copied()
            .unwrap_or_default();
        let wanted: Vec<(u64, BTreeSet<(String, i32)>)> = ids
            .iter()
            .map(|id| {
                let spec = &self.members[id].spec;
                let partitions = self
                    .topics
                    .iter()
                    .filter(|(name, _)| spec.reads(name))
                    .flat_map(|(name, topic)| {
                        (0..topic.partitions.len())
                            .map(move |index| (name.clone(), partition_number(index)))
                    })
                    .collect();
                (*id, partitions)
            })
            .collect();
        let assignment = strategy.assign(&wanted);
        for id in ids {
            let partitions = assignment.get(&id).cloned().unwrap_or_default();
            self.reassign(id, partitions);
        }
    }

    /// Replaces a member's assignment: the old partitions are revoked, the new ones resume from
    /// the group's committed offsets, and a start position the member holds applies to them.
    pub(super) fn reassign(&mut self, id: u64, partitions: BTreeSet<(String, i32)>) {
        let member = self.members.get(&id).expect("a rebalanced member exists");
        for (topic, partition) in member.assignment.keys() {
            member
                .spec
                .tracker
                .reposition(&Str::from(topic.as_str()), *partition);
        }
        let mut assignment = BTreeMap::new();
        let mut failure = None;
        for key in partitions {
            match self.resume_offset(&self.members[&id].spec, &key) {
                Some(offset) => {
                    assignment.insert(key, offset);
                }
                None => failure = Some(Failure::NoOffset),
            }
        }
        let member = self
            .members
            .get_mut(&id)
            .expect("a rebalanced member exists");
        member.assignment = assignment;
        member.cursor = 0;
        if let Some(failure) = failure {
            member.failure.get_or_insert(failure);
        }
        if !member.assignment.is_empty()
            && let Some(start) = member.held_start.take()
        {
            let partitions: Vec<_> = member.assignment.keys().cloned().collect();
            if let Err(err) = self.apply_position(id, &partitions, &start) {
                let member = self.members.get_mut(&id).expect("the member exists");
                let name = &member.spec.name;
                member.failure.get_or_insert_with(|| {
                    Failure::Start(format!(
                        "subscription {name:?} could not be opened at its start position: {err}"
                    ))
                });
            }
        }
    }

    /// Gives a member that names its partitions the ones of them the topic has.
    pub(super) fn assign_named(&mut self, id: u64) {
        let Reader::Assigned { topic, partitions } = &self.members[&id].spec.reader else {
            return;
        };
        let count = self.topics.get(topic).map_or(0, |log| log.partitions.len());
        let keys: Vec<(String, i32)> = partitions
            .iter()
            .filter(|partition| usize::try_from(**partition).is_ok_and(|index| index < count))
            .map(|partition| (topic.clone(), *partition))
            .collect();
        let mut assignment = BTreeMap::new();
        let mut failure = None;
        for key in keys {
            let spec = &self.members[&id].spec;
            let log = &self.topics[&key.0].partitions[usize::try_from(key.1).unwrap_or(0)];
            let offset = match spec.start {
                StartOffset::Earliest => Some(0),
                StartOffset::Latest => Some(log.end()),
                StartOffset::Committed => self.resume_offset(spec, &key),
            };
            match offset {
                Some(offset) => {
                    assignment.insert(key, offset);
                }
                None => failure = Some(Failure::NoOffset),
            }
        }
        let member = self.members.get_mut(&id).expect("the member exists");
        member.assignment = assignment;
        if let Some(failure) = failure {
            member.failure.get_or_insert(failure);
        }
    }

    /// Where a partition resumes for a member: the group's committed offset, else where
    /// `auto.offset.reset` says, or `None` when it says `error`.
    pub(super) fn resume_offset(
        &self,
        spec: &MemberSpec,
        (topic, partition): &(String, i32),
    ) -> Option<i64> {
        let committed = spec
            .group
            .as_ref()
            .and_then(|group| self.committed.get(group))
            .and_then(|offsets| offsets.get(&(topic.clone(), *partition)))
            .copied();
        let end = self.partition(topic, *partition).map_or(0, Partition::end);
        match committed {
            Some(offset) if (0..=end).contains(&offset) => Some(offset),
            _ => spec.reset.offset(end),
        }
    }
}

/// A partition assignment strategy, as `partition.assignment.strategy` names it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum Strategy {
    /// Each topic's partitions in contiguous ranges over the members reading it.
    #[default]
    Range,
    /// Every partition of every topic dealt out in turn.
    RoundRobin,
    /// The incremental protocol. Its placement here is the range one; what it keeps apart from
    /// the eager strategies is that a group cannot mix the two.
    CooperativeSticky,
}

impl Strategy {
    /// The strategies a configured value lists, in preference order.
    pub(crate) fn parse(value: &str) -> Vec<Self> {
        value
            .split(',')
            .filter_map(|name| match name.trim() {
                "range" => Some(Self::Range),
                "roundrobin" => Some(Self::RoundRobin),
                "cooperative-sticky" => Some(Self::CooperativeSticky),
                _ => None,
            })
            .collect()
    }

    fn assign(
        self,
        wanted: &[(u64, BTreeSet<(String, i32)>)],
    ) -> HashMap<u64, BTreeSet<(String, i32)>> {
        let mut assignment: HashMap<u64, BTreeSet<(String, i32)>> = HashMap::new();
        match self {
            Self::Range | Self::CooperativeSticky => {
                let topics: BTreeSet<&String> = wanted
                    .iter()
                    .flat_map(|(_, partitions)| partitions.iter().map(|(topic, _)| topic))
                    .collect();
                for topic in topics {
                    let readers: Vec<u64> = wanted
                        .iter()
                        .filter(|(_, partitions)| partitions.iter().any(|(name, _)| name == topic))
                        .map(|(id, _)| *id)
                        .collect();
                    let partitions: Vec<i32> = wanted
                        .iter()
                        .flat_map(|(_, partitions)| partitions.iter())
                        .filter(|(name, _)| name == topic)
                        .map(|(_, partition)| *partition)
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect();
                    let per = partitions.len() / readers.len();
                    let extra = partitions.len() % readers.len();
                    let mut next = 0;
                    for (rank, reader) in readers.iter().enumerate() {
                        let take = per + usize::from(rank < extra);
                        for partition in &partitions[next..next + take] {
                            assignment
                                .entry(*reader)
                                .or_default()
                                .insert((topic.clone(), *partition));
                        }
                        next += take;
                    }
                }
            }
            Self::RoundRobin => {
                let all: BTreeSet<&(String, i32)> = wanted
                    .iter()
                    .flat_map(|(_, partitions)| partitions.iter())
                    .collect();
                for (turn, key) in all.into_iter().enumerate() {
                    let readers: Vec<u64> = wanted
                        .iter()
                        .filter(|(_, partitions)| partitions.contains(key))
                        .map(|(id, _)| *id)
                        .collect();
                    let reader = readers[turn % readers.len()];
                    assignment.entry(reader).or_default().insert(key.clone());
                }
            }
        }
        assignment
    }
}

/// Where `auto.offset.reset` sends a partition with no valid offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reset {
    Earliest,
    Latest,
    Error,
}

impl Reset {
    /// The value librdkafka resolved, in any of the spellings it accepts.
    pub(crate) fn parse(value: &str) -> Self {
        match value {
            "smallest" | "earliest" | "beginning" => Self::Earliest,
            "error" => Self::Error,
            _ => Self::Latest,
        }
    }

    pub(super) fn offset(self, end: i64) -> Option<i64> {
        match self {
            Self::Earliest => Some(0),
            Self::Latest => Some(end),
            Self::Error => None,
        }
    }
}
