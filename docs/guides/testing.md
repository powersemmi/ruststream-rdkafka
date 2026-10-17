---
sidebar_position: 9
description: "Test a Kafka service in process with TestApp: the production app, keyed records, replies, delays, and the same test against a cluster."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing a Kafka service

A test runs the service's own app with [`TestApp`](api:ruststream::testing::TestApp), against an in-process Kafka or against a cluster.

The service under test charges card payments.
A payment arrives on `card-payments`, and its record key is the account to charge.
The handler replies with a receipt on `card-receipts`.
A payment without a key is dropped.
A payment over the bank's instant limit is held: the handler asks for it again in a minute with `retry_after`.
After three deliveries a payment goes to `card-payments.review`, the dead-letter topic.
The app is built by `service(broker)`, so a test chooses the broker and `main` uses the production one.
The tests need the `testing` feature of `ruststream-rdkafka` in `[dev-dependencies]`.

## A keyed payment

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) connects `KafkaBroker` to an in-process Kafka.
The bootstrap address is never dialled.
The test addresses the broker by its production type, `tb.broker::<KafkaBroker>()`.
A Kafka record key travels as the [`PARTITION_KEY_HEADER`](api:ruststream_rdkafka::PARTITION_KEY_HEADER) header, so the test sets it with `with_headers`.
The broker turns that header into the record's native key, and the handler reads it back with `Ctx<Key>`.
`publish` returns once the handler and its reply have settled.
So the next lines assert on finished state: how the subscriber settled and what reached `card-receipts`.
The body takes the started app as an argument, so the live test below runs it too.

## A payment without a key

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#keyless"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#keyless"
```

</TabItem>
</Tabs>

`settled` checks the outcome the handler returned.
`assert_not_called` checks that no receipt was published.

## A delay, without waiting for it

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

The test starts on a paused clock, and [`advance`](api:ruststream::testing::TestApp::advance) moves it forward.
A minute of `HOLD` costs nothing.
Kafka cannot hold a record back, so `retry_after` publishes a copy of the record to its topic when the delay ends.
The copy carries the retry count in a header, and `published` shows it on `card-payments` next to the test's own record.
The third delivery is the last one the mount site allows, so the record goes to `card-payments.review` as it arrived.
Each held delivery is recorded as `Outcome::Nack`.

## What the in-process Kafka models

The in-process Kafka reads the configuration librdkafka would read: the broker's `config` and `producer_config`, each descriptor's options, and librdkafka's defaults.
A property librdkafka refuses fails the test's start the same way.
It models Kafka rather than a queue:

- Each consumer group reads a record once, and every group reads its own copy.
- The commit mode decides what is committed: a `retry` under [`Commit::Tracked`](api:ruststream_rdkafka::Commit::Tracked) comes back, and under [`Commit::Auto`](api:ruststream_rdkafka::Commit::Auto) it reports itself unsupported.
- A record key places the record and comes back to the handler.
- A seek moves a subscription to an offset, either end of the log, or a timestamp.
- A `read_committed` reader sees a transaction's records when it commits and never after an abort.

Every topic has one partition there.
Several partitions, cooperative rebalancing, retention and the timing of a real group join need a cluster.

## The same test on a cluster

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live_within`](api:ruststream::testing::TestApp::start_live_within) connects the same app to a running Kafka.
Its deadline is the longest a publish waits for the reaction to settle; a real cluster needs more than the in-process default.
The test reads the address from `KAFKA_TEST_URL` with `live::url`, a helper of the example's own, and skips when it is unset.
It joins a consumer group of its own, so nothing an earlier run left behind is read.
It creates `card-payments` first: a subscription opens on the partitions a topic has, and a topic that appears later is read from where `auto.offset.reset` puts it.
The assertions are the in-process ones, unchanged.

```bash
cargo test
KAFKA_TEST_URL=localhost:9092 cargo test -- --test-threads=1
```
