---
sidebar_position: 10
description: "Connect a Kafka service to a production cluster: SASL and TLS, producer durability, timeouts, per-subscription overrides and a test of the configuration."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Connecting to a production cluster

A production cluster asks for [authentication and TLS](../components/broker/security.md), and a service sets its producer's [durability](../components/publishing/durability.md) and its own timeouts.

## SASL over TLS

{/* single-form: the broker's configuration is the same code with and without the macros */}
```rust title="src/main.rs" reference="examples/guide/production.rs#broker"
```

```toml title="Cargo.toml"
[dependencies]
ruststream-rdkafka = { version = "0.7", features = ["ssl"] }
```

The deployment provides the address and the credentials in environment variables.
Every librdkafka property goes through [`KafkaBroker::config`](../components/broker/config.md), which applies it to the consumers and the producer.
`security.protocol` selects SASL over TLS, and `sasl.mechanism` the SASL mechanism.
TLS needs librdkafka built with OpenSSL: the `ssl` feature links the system OpenSSL, and `ssl-vendored` builds one from source.
SCRAM and OAUTHBEARER need the same feature, because librdkafka builds them on OpenSSL.
SASL `PLAIN` over `SASL_PLAINTEXT` needs no feature.
Kerberos (`GSSAPI`) is the `gssapi` feature of [rust-rdkafka](https://docs.rs/rdkafka): add `rdkafka` to your dependencies with that feature.
A cluster with TLS and no SASL takes `security.protocol` set to `SSL`, and the `ssl.*` properties name the certificates.

## Durability and timeouts

[`producer_config`](api:ruststream_rdkafka::KafkaBroker::producer_config) sets a property on the producer only.
`acks=all` makes the cluster confirm a record once every in-sync replica has it.
`enable.idempotence` keeps a producer retry from writing a record twice.
Every publish waits for the cluster's confirmation, so a successful publish means the cluster accepted the record.

The broker has timeouts of its own:

- [`connect_timeout`](../components/broker/startup.md) bounds the check that the cluster is reachable at startup.
- [`flush_timeout`](../components/broker/shutdown.md) bounds the flush of unsent records at shutdown.
- [`assignment_timeout`](api:ruststream_rdkafka::KafkaBroker::assignment_timeout) bounds how long startup waits for the group to assign a subscription its partitions.

## The service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

A publish waits for space in librdkafka's local queue when the queue is full.
[`queue_timeout`](api:ruststream_rdkafka::KafkaPublish::queue_timeout) on the [publish policy](../components/publishing/index.md) turns a longer wait into an error.

A subscription sets its own properties with [`KafkaTopic::config`](../components/subscriptions/passthrough.md), and they win over the broker's.
Two properties belong to the [commit mode](../components/settlement/index.md): `enable.auto.commit` and `enable.auto.offset.store`.
Under [`Commit::Tracked`](../components/settlement/tracked.md) and `Commit::Transactional` a subscription that sets either refuses to start, and the error names the property and the mode.

## Testing the configuration

The tests start the production `service` with the production `broker`, given test credentials, under `TestApp::start`.
The [in-process Kafka](../components/testing/index.md) validates the configuration the way librdkafka does, without dialling the cluster.
A property name or value that librdkafka refuses fails the start with librdkafka's message:

```text
Invalid value "SASL_TLS" for configuration property "security.protocol"
```

The tests need the same features as the service.
librdkafka built without OpenSSL refuses `SASL_SSL` and `SSL` as values of `security.protocol`.
Authentication itself happens only on a real cluster.
