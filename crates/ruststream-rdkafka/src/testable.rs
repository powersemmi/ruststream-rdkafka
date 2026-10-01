//! The test harness's view of a connected broker, behind the `testing` feature: what it injects,
//! what it reads back, and where a publish is delivered.
//!
//! # Where a publish is delivered
//!
//! A record produced to a topic reaches every consumer group reading that topic, once per group,
//! and each subscription that names its partitions on its own, once each. Which subscriptions read
//! a topic is decided the way the consumer decides it: a topic a subscription names, a topic a
//! `^`-anchored pattern of it matches, and the topic of a partition list. Within a group the
//! record is owed by the first subscription of that group that reads the topic, because the
//! harness counts deliveries per subscription name and a group hands the record to one member.
//!
//! Two limits follow from reading the configuration rather than the cluster. A partition list is
//! owed every record of its topic, since the partition a live record lands on is the cluster's
//! placement. And a group reading one topic through subscriptions of different names is owed by
//! the first of them, while the cluster may hand the partition to another.

use std::sync::Mutex;

use regex::Regex;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{OutgoingMessage, RawMessage};

use crate::broker::ConnectedKafkaBroker;
use crate::in_process::Cluster;
use crate::subscription::{Reader, SubscriptionPlan};

/// What every subscription of one connection reads.
#[derive(Debug, Default)]
pub(crate) struct Routes {
    entries: Mutex<Vec<Route>>,
}

#[derive(Debug)]
struct Route {
    /// The subscription's name, which the harness counts its deliveries under.
    name: String,
    group: Option<String>,
    reads: Reads,
}

#[derive(Debug)]
enum Reads {
    /// Through the group: these topics, and every topic these patterns match.
    Group {
        topics: Vec<String>,
        patterns: Vec<Regex>,
    },
    /// A partition list of one topic, read apart from any group.
    Partitions(String),
}

impl Reads {
    fn topic(&self, destination: &str) -> bool {
        match self {
            Self::Group { topics, patterns } => {
                topics.iter().any(|topic| topic == destination)
                    || patterns.iter().any(|pattern| pattern.is_match(destination))
            }
            Self::Partitions(topic) => topic == destination,
        }
    }
}

impl Routes {
    /// Notes what a subscription the connection opens reads.
    pub(crate) fn record(&self, plan: &SubscriptionPlan, group: Option<&str>) {
        let reads = match &plan.reader {
            Reader::Subscribed(names) => Reads::Group {
                topics: names
                    .iter()
                    .filter(|name| !name.starts_with('^'))
                    .cloned()
                    .collect(),
                // A pattern that does not compile is refused when the subscription opens, so
                // here it simply matches nothing.
                patterns: names
                    .iter()
                    .filter(|name| name.starts_with('^'))
                    .filter_map(|pattern| Regex::new(pattern).ok())
                    .collect(),
            },
            Reader::Assigned { topic, .. } => Reads::Partitions(topic.clone()),
        };
        self.entries
            .lock()
            .expect("routes mutex poisoned")
            .push(Route {
                name: plan.name.clone(),
                group: group.map(str::to_owned),
                reads,
            });
    }

    /// The positions in `subscriptions` a record produced to `destination` is owed by (see the
    /// module documentation for the rules).
    pub(crate) fn answer(&self, destination: &str, subscriptions: &[&str]) -> Vec<usize> {
        let entries = self.entries.lock().expect("routes mutex poisoned");
        let mut taken = vec![false; subscriptions.len()];
        let mut groups_served: Vec<&str> = Vec::new();
        let mut owed = Vec::new();
        for route in entries
            .iter()
            .filter(|route| route.reads.topic(destination))
        {
            let names: Vec<&str> = match (&route.reads, &route.group) {
                (Reads::Group { .. }, Some(group)) => {
                    if groups_served.contains(&group.as_str()) {
                        continue;
                    }
                    groups_served.push(group);
                    entries
                        .iter()
                        .filter(|other| {
                            other.group.as_deref() == Some(group.as_str())
                                && matches!(other.reads, Reads::Group { .. })
                                && other.reads.topic(destination)
                        })
                        .map(|other| other.name.as_str())
                        .collect()
                }
                _ => vec![route.name.as_str()],
            };
            let position = subscriptions
                .iter()
                .enumerate()
                .find(|(index, name)| !taken[*index] && names.contains(name))
                .map(|(index, _)| index);
            if let Some(position) = position {
                taken[position] = true;
                owed.push(position);
            }
        }
        owed.sort_unstable();
        owed
    }
}

/// The harness drives the in-process cluster: it installs its coordinator there, injects a test's
/// input as an external producer would, and reads the topic back. Where a publish is delivered is
/// answered for both modes, from what the connection's subscriptions read.
///
/// # Panics
///
/// `inject` and `published` panic on a broker connected with `connect`: the harness drives only
/// the connection `connect_in_process` produced, and a live cluster has no log to read and no
/// synchronous way to take a record. `inject` also panics when the cluster refuses the record,
/// as a real producer would report it.
impl TestableBroker for ConnectedKafkaBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        if let Some(cluster) = self.state().in_process() {
            cluster.install(coordinator);
        }
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        let cluster = self.cluster("inject");
        if let Err(err) = cluster.produce(
            message.name(),
            None,
            message.payload(),
            message.headers(),
            None,
        ) {
            panic!(
                "the injected record for {:?} is not one the cluster takes: {err}",
                message.name()
            );
        }
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.cluster("published").published(name)
    }

    fn routes(&self, destination: &str, subscriptions: &[&str]) -> Vec<usize> {
        self.state().routes.answer(destination, subscriptions)
    }
}

impl ConnectedKafkaBroker {
    /// The in-process cluster, which is all the harness drives.
    fn cluster(&self, what: &str) -> &Cluster {
        self.state().in_process().unwrap_or_else(|| {
            panic!(
                "TestableBroker::{what} reached a broker connected with `connect`; the harness \
                 drives the connection `connect_in_process` produces"
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::GroupSettings;

    fn plan(name: &str, reader: Reader) -> SubscriptionPlan {
        SubscriptionPlan {
            name: name.to_owned(),
            reader,
            settings: GroupSettings::default(),
        }
    }

    fn subscribed(names: &[&str]) -> Reader {
        Reader::Subscribed(names.iter().map(|name| (*name).to_owned()).collect())
    }

    #[test]
    fn a_record_is_owed_once_per_group_reading_its_topic() {
        let routes = Routes::default();
        routes.record(&plan("orders", subscribed(&["orders"])), Some("billing"));
        routes.record(&plan("orders", subscribed(&["orders"])), Some("billing"));
        routes.record(&plan("orders", subscribed(&["orders"])), Some("audit"));
        routes.record(
            &plan("payments", subscribed(&["payments"])),
            Some("billing"),
        );
        let subscriptions = ["orders", "orders", "orders", "payments"];
        assert_eq!(routes.answer("orders", &subscriptions), [0, 1]);
        assert_eq!(routes.answer("payments", &subscriptions), [3]);
        assert!(routes.answer("refunds", &subscriptions).is_empty());
    }

    #[test]
    fn a_pattern_and_a_topic_list_read_the_topics_they_match() {
        let routes = Routes::default();
        routes.record(
            &plan("^orders\\..*", subscribed(&["^orders\\..*"])),
            Some("a"),
        );
        routes.record(&plan("eu,us", subscribed(&["eu", "us"])), Some("b"));
        routes.record(
            &plan(
                "eu",
                Reader::Assigned {
                    topic: "eu".to_owned(),
                    partitions: vec![0],
                },
            ),
            None,
        );
        let subscriptions = ["^orders\\..*", "eu,us", "eu"];
        assert_eq!(routes.answer("orders.eu", &subscriptions), [0]);
        assert!(routes.answer("orders", &subscriptions).is_empty());
        assert_eq!(routes.answer("eu", &subscriptions), [1, 2]);
        assert_eq!(routes.answer("us", &subscriptions), [1]);
    }
}
