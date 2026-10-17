---
sidebar_position: 1
title: Live
description: "TestApp::start_live runs the same test body against a running Kafka, for what only a cluster has."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Live

[`TestApp::start_live`](api:ruststream::testing::TestApp::start_live) runs the same test body against a running Kafka, for what only a cluster has.

It connects the broker through its ordinary connect, so startup probes the cluster and every group joins it for real.
The body is the one an in-process test runs; only the start call differs.
A publish, `settle` and `advance` wait on the real clock until every subscription has handled what was published to it.
The wait lasts ten seconds at most, and [`start_live_within`](api:ruststream::testing::TestApp::start_live_within) sets another bound.
A consumer group takes a few seconds to form on a cluster, so a Kafka test gives it thirty.

The live mode is for what the in-process cluster leaves out: a topic created with several partitions, the cooperative assignment that keeps partitions sticky, retention, and the timing of a group join.
A cluster keeps what earlier runs left in it.
Each run reads in a consumer group of its own, and creates its topics before the app starts: a group assigned a topic created after it subscribed reads from the end of the log, past the record that created the topic.

In the example, `live::url` returns the cluster's address from `KAFKA_TEST_URL`, and the test returns early without one, so `cargo test` passes on a machine with no cluster.
A job that stood a cluster up sets `RUSTSTREAM_REQUIRE_LIVE=1`, and a missing address then fails the test instead of skipping it.
The repository's stand is `docker-compose.test.yml`, which `just brokers-up` starts.

## Example

The two tests share one body: one in process, one against the cluster at `KAFKA_TEST_URL`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_testing_live.rs#start-live"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_testing_live.rs#start-live"
```

</TabItem>
</Tabs>
