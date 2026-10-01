//! Conformance: the production broker's in-process mode passes `run_suite` and every suite that
//! takes a `Broker`, wrapped in `InProcessBroker`; the same suites run against a real Kafka when
//! `KAFKA_TEST_URL` is set (see `docker-compose.test.yml` and `just test-brokers`), together with
//! the ones that hold the in-process transport to the server.

#![cfg(feature = "testing")]
// The suites take higher-ranked closures that method paths cannot satisfy.
#![allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]

use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::RDKafkaErrorCode;
use ruststream::conformance::harness::InProcessBroker;
use ruststream::conformance::helpers::unique_subject;
use ruststream::conformance::in_process::Refusal;
use ruststream::conformance::message_shape::OptionCases;
use ruststream::conformance::{
    capabilities, harness, in_process, lifecycle, message_shape, retry, settlement,
};
use ruststream::testing::Backlog;
use ruststream::{Bytes, HeaderMap, Name};
use ruststream_rdkafka::{
    Commit, KafkaBroker, KafkaMessage, KafkaOptions, KafkaPosition, KafkaPublish, KafkaTopic,
    LaneKey, PARTITION_KEY_HEADER, StartOffset,
};
use tokio::runtime::Handle;
use tokio::task;

mod live;

fn kafka_url() -> Option<String> {
    live::url("KAFKA_TEST_URL")
}

/// Creates `topic` with `partitions` partitions on the cluster, accepting a topic that is
/// already there.
async fn create_topic_with(url: &str, topic: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", url)
        .create()
        .expect("admin client");
    let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
    let results = admin
        .create_topics([&new_topic], &AdminOptions::new())
        .await
        .expect("create_topics call");
    for result in results {
        match result {
            Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((name, code)) => panic!("creating topic {name} failed: {code}"),
        }
    }
}

/// Creates `topic` on the cluster with one partition, accepting a topic that is already there.
async fn create_topic(url: &str, topic: &str) {
    create_topic_with(url, topic, 1).await;
}

/// Creates `topic` from inside a synchronous suite factory.
fn create_topic_now(url: &str, topic: &str, partitions: i32) {
    task::block_in_place(|| Handle::current().block_on(create_topic_with(url, topic, partitions)));
}

/// The production broker as a service configures it.
fn service_broker() -> KafkaBroker {
    KafkaBroker::new(["kafka:9092"]).default_group("tests")
}

/// The production broker as a service configures it; in process its address is never dialled.
fn in_process() -> InProcessBroker<KafkaBroker> {
    InProcessBroker::new(service_broker())
}

/// The in-process suites' descriptor: a group of their own, reading from the start of the log
/// and settling by the tracked commit, as the live suites' descriptor does.
fn in_process_topic(name: &str) -> KafkaTopic {
    KafkaTopic::new(name)
        .group("conformance")
        .start(StartOffset::Earliest)
        .commit(Commit::Tracked)
}

/// Where a keyed delivery reports the key it was published under.
fn keyed(topic: KafkaTopic) -> KafkaTopic {
    topic.lane_key(LaneKey::RecordKey)
}

/// How a publish carries a record key: the core's partition key header, which the publisher maps
/// onto the native key.
fn key_header(key: &[u8], headers: &mut HeaderMap) -> Option<KafkaOptions> {
    headers.insert(PARTITION_KEY_HEADER, Bytes::copy_from_slice(key));
    None
}

/// The per-record settings of a publish over a one-partition topic: pinning the one partition
/// lands there, and a partition the topic does not have fails the publish.
fn partition_cases() -> OptionCases<KafkaOptions, i32> {
    OptionCases::new(0)
        .overrides(KafkaOptions::default().partition(0), 0)
        .refuses(KafkaOptions::default().partition(5))
}

// ----------------------------------------------------------------------------- in process

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_broker_passes_the_routing_suite() {
    harness::run_suite(service_broker).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_broker_passes_lifecycle() {
    harness::lifecycle(
        in_process,
        |name| KafkaTopic::new(name).group("conformance"),
        |connected| connected.publisher(KafkaPublish::default()),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_broker_reports_a_reachable_redelivery_address() {
    retry::redelivery_address(
        in_process,
        |name| KafkaTopic::new(name).group("conformance"),
        |connected| connected.publisher(KafkaPublish::default()),
    )
    .await;
}

/// `Subscribe::Copies` is `AddressedCopies`, so a bare name is the address of the attribute
/// form's retry copies.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_broker_reports_a_reachable_redelivery_address_for_a_name() {
    retry::redelivery_address(
        in_process,
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(KafkaPublish::default()),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_broker_holds_tracked_settlements_to_their_meaning() {
    settlement::suite(
        in_process,
        in_process_topic,
        |connected| connected.publisher(KafkaPublish::default()),
        Duration::ZERO,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_broker_holds_auto_commit_settlements_to_their_meaning() {
    settlement::suite(
        in_process,
        |name| KafkaTopic::new(name).group("conformance"),
        |connected| connected.publisher(KafkaPublish::default()),
        Duration::ZERO,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_broker_passes_batches() {
    capabilities::batches(in_process, in_process_topic, |connected| {
        connected.publisher(KafkaPublish::default())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_broker_passes_batch_seeking() {
    capabilities::batch_seeking(in_process, in_process_topic, |connected| {
        connected.publisher(KafkaPublish::default())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_broker_passes_transactions() {
    capabilities::transactions(in_process, in_process_topic, |connected| {
        let policy = KafkaPublish::default().transactional_id("conformance-tx");
        task::block_in_place(|| {
            Handle::current().block_on(connected.transactional_publisher(policy))
        })
        .expect("transactional publisher must pair")
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_broker_passes_seeking() {
    capabilities::seeking(in_process, in_process_topic, |connected| {
        connected.publisher(KafkaPublish::default())
    })
    .await;
}

/// A partition the topic does not have is a position its log cannot hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_broker_refuses_a_seek_to_an_unknown_position() {
    capabilities::seeking_unknown_position(
        in_process,
        in_process_topic,
        |connected| connected.publisher(KafkaPublish::default()),
        |subject| KafkaPosition::topic_offset(subject, 7, 0),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_broker_keeps_the_order_of_a_key() {
    message_shape::keyed_order(
        in_process,
        &unique_subject("conformance.keyed"),
        |name| keyed(in_process_topic(name)),
        |connected| connected.publisher(KafkaPublish::default()),
        key_header,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_broker_resolves_publish_options() {
    message_shape::publish_options(
        in_process,
        &unique_subject("conformance.options"),
        in_process_topic,
        KafkaPublish::default(),
        partition_cases(),
        |msg: &KafkaMessage| msg.partition(),
    )
    .await;
}

// ----------------------------------------------------------------------------- the document

#[cfg(feature = "asyncapi")]
#[test]
fn describes_its_addresses_without_credentials() {
    message_shape::describes_addresses_without_credentials(
        |addrs| KafkaBroker::new(addrs.iter().copied()),
        "SASL_SSL",
    );
}

/// The one publish policy that holds a credential: the registry a framed publish registers its
/// schemas with, configured from a URL that carries its user and password.
#[cfg(all(feature = "asyncapi", feature = "protobuf"))]
#[test]
fn a_framed_publish_describes_itself_without_the_registry_credentials() {
    use ruststream_rdkafka::{ConnectedKafkaBroker, SchemaRegistry};

    let registry = SchemaRegistry::new("http://svc:hunter2@registry.internal:8081");
    message_shape::publishes_without_credentials::<ConnectedKafkaBroker, _>(
        &KafkaPublish::framed(&registry),
        "hunter2",
    );
}

// ----------------------------------------------------------------------------- live

/// The subscription descriptor a suite subscribes through, over a topic created first.
///
/// Every suite publishes under a subject of its own, generated per run, and this factory is the
/// first place that name is known: creating the topic here puts it on the cluster before the
/// subscription opens. A consumer that subscribes to a name the cluster does not know waits for
/// a metadata refresh to notice the topic auto-created by the publish, and that wait is longer
/// than the suite's delivery deadline.
fn topic_created(url: &str, group: &str, name: &str) -> KafkaTopic {
    create_topic_now(url, name, 1);
    KafkaTopic::new(name)
        .group(group.to_owned())
        .start(StartOffset::Earliest)
        .commit(Commit::Tracked)
}

/// A group of its own for one live test.
fn group(test: &str) -> String {
    format!("conformance-{test}-{}", std::process::id())
}

/// The first consumer group on a fresh cluster makes the broker create `__consumer_offsets`
/// (50 partitions), which can take longer than the harness's short delivery deadline. A
/// throwaway group round-trip absorbs that one-time cost with a generous timeout.
async fn warm_up_group_coordinator(url: &str) {
    use futures::StreamExt as _;
    use ruststream::{Broker as _, ConnectedBroker as _, IncomingMessage as _};
    use ruststream::{OutgoingMessage, Publisher as _};
    use ruststream::{Subscriber as _, SubscriptionSource as _};

    let scratch = format!("conformance-warmup-{}", std::process::id());
    create_topic(url, &scratch).await;
    let broker = KafkaBroker::new([url.to_owned()])
        .connect()
        .await
        .expect("warm-up connect");
    let mut subscriber = KafkaTopic::new(&scratch)
        .group(&scratch)
        .start(StartOffset::Earliest)
        .subscribe(&broker)
        .await
        .expect("warm-up subscribe");
    broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new(&scratch, b"warm-up"), None)
        .await
        .expect("warm-up publish");
    let mut stream = Box::pin(subscriber.stream());
    let msg = tokio::time::timeout(Duration::from_secs(30), stream.next())
        .await
        .expect("warm-up delivery within timeout")
        .expect("warm-up stream has next")
        .expect("warm-up delivery ok");
    msg.ack().await.expect("warm-up ack");
    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("warm-up shutdown");
}

/// The cluster's address once the group coordinator is up, or `None` to skip.
async fn live_cluster() -> Option<String> {
    let url = kafka_url()?;
    warm_up_group_coordinator(&url).await;
    Some(url)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_lifecycle() {
    let Some(url) = live_cluster().await else {
        return;
    };
    let group = group("lifecycle");
    let servers = url.clone();
    harness::lifecycle(
        move || KafkaBroker::new([servers.clone()]),
        |name| topic_created(&url, &group, name),
        |connected| connected.publisher(KafkaPublish::default()),
    )
    .await;
}

/// `KafkaTopic` is the crate's one addressed descriptor, and this is the promise it makes: the
/// topic it reports is a topic a publish on this broker reaches it through, which is exactly
/// what the runtime does with a deferred `retry_after` copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_redelivery_address() {
    let Some(url) = live_cluster().await else {
        return;
    };
    let group = group("redelivery");
    retry::redelivery_address(
        || KafkaBroker::new([url.clone()]),
        |name| topic_created(&url, &group, name),
        |connected| connected.publisher(KafkaPublish::default()),
    )
    .await;
}

/// A bare name is the address of the attribute form's retry copies, and its subscription reads
/// with the broker's default group and librdkafka's default `latest` reset: the copy and the
/// control message are published once both subscriptions of the group are open.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_redelivery_address_for_a_name() {
    let Some(url) = live_cluster().await else {
        return;
    };
    let group = group("redelivery-name");
    retry::redelivery_address(
        || KafkaBroker::new([url.clone()]).default_group(group.clone()),
        |name| {
            create_topic_now(&url, name, 1);
            Name::new(name.to_owned())
        },
        |connected| connected.publisher(KafkaPublish::default()),
    )
    .await;
}

/// The in-process cluster's backlog declaration is what the server does with a subscription
/// opened by name in a fresh group, under librdkafka's default `latest` reset: what was published
/// before it opened is missed, what is published after reaches it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_backlog_matches_the_server() {
    let Some(url) = live_cluster().await else {
        return;
    };
    let group = group("backlog");
    in_process::backlog_matches_server(
        || KafkaBroker::new([url.clone()]).default_group(group.clone()),
        |connected| connected.publisher(KafkaPublish::default()),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_flushes_acknowledgements_and_publishes() {
    let Some(url) = live_cluster().await else {
        return;
    };
    let group = group("flush");
    lifecycle::shutdown_flushes(
        || KafkaBroker::new([url.clone()]),
        |name| topic_created(&url, &group, name),
        |connected| connected.publisher(KafkaPublish::default()),
        Backlog::Delivered,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tracked_settlements_match_in_process() {
    let Some(url) = live_cluster().await else {
        return;
    };
    let group = group("settlement-tracked");
    settlement::matches_in_process(
        || KafkaBroker::new([url.clone()]),
        |name| topic_created(&url, &group, name),
        |connected| connected.publisher(KafkaPublish::default()),
        Duration::ZERO,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_commit_settlements_match_in_process() {
    let Some(url) = live_cluster().await else {
        return;
    };
    let group = group("settlement-auto");
    settlement::matches_in_process(
        || KafkaBroker::new([url.clone()]),
        |name| {
            create_topic_now(&url, name, 1);
            KafkaTopic::new(name)
                .group(group.clone())
                .start(StartOffset::Earliest)
        },
        |connected| connected.publisher(KafkaPublish::default()),
        Duration::ZERO,
    )
    .await;
}

/// What the cluster refuses: a topic name outside its grammar, to publish to and to subscribe
/// to, and a record one byte over what the producer hands the cluster (`message.max.bytes`, less
/// the 36 bytes librdkafka reserves for a record's framing).
///
/// The size probe reads its record back through a subscription opened by name, which joins the
/// group before it reads, so the broker reads a partition the group has not committed from its
/// start.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_transport_refuses_what_the_server_refuses() {
    let Some(url) = live_cluster().await else {
        return;
    };
    let group = group("refusals");
    let sized = unique_subject("conformance.refusals.size");
    create_topic(&url, &sized).await;
    in_process::refuses_like_the_server(
        || {
            KafkaBroker::new([url.clone()])
                .default_group(group.clone())
                .config("auto.offset.reset", "earliest")
        },
        |connected| connected.publisher(KafkaPublish::default()),
        [
            Refusal::Publish {
                name: "conformance refused topic".to_owned(),
            },
            Refusal::Subscription {
                source: KafkaTopic::new("conformance refused topic").group(group.clone()),
            },
            Refusal::PayloadOver {
                name: sized,
                limit: 999_964,
            },
        ],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_batches_capability() {
    let Some(url) = live_cluster().await else {
        return;
    };
    let group = group("batches");
    capabilities::batches(
        || KafkaBroker::new([url.clone()]),
        |name| topic_created(&url, &group, name),
        |connected| connected.publisher(KafkaPublish::default()),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_batch_seeking_capability() {
    let Some(url) = live_cluster().await else {
        return;
    };
    let group = group("batch-seeking");
    capabilities::batch_seeking(
        || KafkaBroker::new([url.clone()]),
        |name| topic_created(&url, &group, name),
        |connected| connected.publisher(KafkaPublish::default()),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_transactions_capability() {
    let Some(url) = live_cluster().await else {
        return;
    };
    let group = group("tx-group");
    let tx_id = self::group("tx");
    capabilities::transactions(
        || KafkaBroker::new([url.clone()]),
        |name| topic_created(&url, &group, name),
        |connected| {
            // The harness factory is synchronous, while a Kafka transactional publisher does
            // real work when it comes alive (`init_transactions` fences earlier producers with
            // the same id, blocking), so the pairing is driven to completion here.
            let policy = KafkaPublish::default().transactional_id(tx_id.clone());
            task::block_in_place(|| {
                Handle::current().block_on(connected.transactional_publisher(policy))
            })
            .expect("transactional publisher must pair")
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_seeking_capability() {
    let Some(url) = live_cluster().await else {
        return;
    };
    let group = group("seeking");
    capabilities::seeking(
        || KafkaBroker::new([url.clone()]),
        |name| topic_created(&url, &group, name),
        |connected| connected.publisher(KafkaPublish::default()),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refuses_a_seek_to_an_unknown_position() {
    let Some(url) = live_cluster().await else {
        return;
    };
    let group = group("seeking-unknown");
    capabilities::seeking_unknown_position(
        || KafkaBroker::new([url.clone()]),
        |name| topic_created(&url, &group, name),
        |connected| connected.publisher(KafkaPublish::default()),
        |subject| KafkaPosition::topic_offset(subject, 7, 0),
    )
    .await;
}

/// Three partitions, so the keys spread and the order check is about each key, not the log.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keeps_the_order_of_a_key() {
    let Some(url) = live_cluster().await else {
        return;
    };
    let group = group("keyed");
    message_shape::keyed_order(
        || KafkaBroker::new([url.clone()]),
        &unique_subject("conformance.keyed"),
        |name| {
            create_topic_now(&url, name, 3);
            keyed(
                KafkaTopic::new(name)
                    .group(group.clone())
                    .start(StartOffset::Earliest)
                    .commit(Commit::Tracked),
            )
        },
        |connected| connected.publisher(KafkaPublish::default()),
        key_header,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolves_publish_options() {
    let Some(url) = live_cluster().await else {
        return;
    };
    let group = group("options");
    message_shape::publish_options(
        || KafkaBroker::new([url.clone()]),
        &unique_subject("conformance.options"),
        |name| topic_created(&url, &group, name),
        KafkaPublish::default(),
        partition_cases(),
        |msg: &KafkaMessage| msg.partition(),
    )
    .await;
}
