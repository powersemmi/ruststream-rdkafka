//! Opening a subscription while the cluster moves its partition leaders, against librdkafka's
//! mock cluster: the cluster answers the requests that read where each partition starts with a
//! retriable error first, as a real one does while a fresh topic elects its leaders or a broker
//! hands its partitions over. The subscription opens once the leaders answer.

use std::time::Duration;

use rdkafka::mocking::MockCluster;
use rdkafka::producer::DefaultProducerContext;
use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
use ruststream::{Broker, ConnectedBroker, Seekable as _, Seeker as _};
use ruststream_rdkafka::{
    ConnectedKafkaBroker, KafkaBroker, KafkaPartitions, KafkaPosition, KafkaTopic, StartOffset,
};

/// How many times in a row the mock cluster refuses to name where a partition ends. librdkafka
/// does not repeat the request on its own, so a single refusal reaches the subscription.
const REFUSALS: usize = 3;

/// The cluster librdkafka runs in process, with one broker.
type Cluster = MockCluster<'static, DefaultProducerContext>;

fn leader_moving(cluster: &Cluster) {
    cluster.request_errors(
        RDKafkaApiKey::ListOffsets,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION; REFUSALS],
    );
}

/// A broker connected to the cluster at `bootstrap`. It takes the address rather than the
/// cluster, whose handle cannot cross threads.
async fn connected(bootstrap: String) -> ConnectedKafkaBroker {
    KafkaBroker::new([bootstrap])
        .assignment_timeout(Duration::from_secs(30))
        .connect()
        .await
        .expect("connect")
}

/// A group subscription opens although the first reads of its partitions' ends meet a leader
/// that moved.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_subscription_opens_while_a_leader_moves() {
    let cluster = Cluster::new(1).expect("mock cluster");
    cluster.create_topic("moving", 1, 1).expect("topic");
    let broker = connected(cluster.bootstrap_servers()).await;
    leader_moving(&cluster);

    let subscriber = broker
        .subscribe_with(KafkaTopic::new("moving").group("moving-group"))
        .await
        .expect("a retriable refusal of the start positions must not fail the subscription");

    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}

/// A subscription that names its partitions opens although the first reads of their ends meet a
/// leader that moved.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn named_partitions_open_while_a_leader_moves() {
    let cluster = Cluster::new(1).expect("mock cluster");
    cluster.create_topic("moving-named", 1, 1).expect("topic");
    let broker = connected(cluster.bootstrap_servers()).await;
    leader_moving(&cluster);

    let subscriber = broker
        .subscribe_with(KafkaPartitions::new("moving-named", [0]).start(StartOffset::Latest))
        .await
        .expect("a retriable refusal of the start positions must not fail the subscription");

    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}

/// A seek to the log's end, or to a timestamp, lands although the first reads of where the
/// partition ends meet a leader that moved.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seek_lands_while_a_leader_moves() {
    let cluster = Cluster::new(1).expect("mock cluster");
    cluster.create_topic("moving-seek", 1, 1).expect("topic");
    let broker = connected(cluster.bootstrap_servers()).await;
    let subscriber = broker
        .subscribe_with(KafkaPartitions::new("moving-seek", [0]).start(StartOffset::Latest))
        .await
        .expect("subscribe");
    let seeker = subscriber.seeker();

    for to in [KafkaPosition::Latest, KafkaPosition::Timestamp(0)] {
        leader_moving(&cluster);
        seeker
            .seek(to)
            .await
            .expect("a retriable refusal of the log's end must not fail the seek");
    }

    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}
