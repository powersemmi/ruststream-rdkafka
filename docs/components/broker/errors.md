---
sidebar_position: 6
title: Errors
description: "KafkaError is the one error type of the crate: connect, publish, consume, transactions, the registry and the wire format."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Errors

[`KafkaError`](api:ruststream_rdkafka::KafkaError) is the one error type of the crate: connect, publish, consume, transactions, the registry and the wire format.

Each variant names the step that failed.
The librdkafka error behind it is its `source`, so the client library's types stay out of the crate's API.

| Variant | When |
|---|---|
| `Connect` | the client could not be created, a property was refused, or the cluster did not answer the startup probe |
| `Subscribe` | a consumer could not be created or subscribed, or its group did not assign it partitions in time |
| `Consume` | an open consumer failed to receive |
| `Publish` | the cluster refused a record or did not confirm it, or the shutdown flush ran out of time |
| `Closed` | a publisher or a transaction was used after the broker shut down |
| `InvalidOptions` | the settings cannot work together, such as a subscription with no consumer group |
| `TransactionBusy`, `NoTransaction` | a transaction was begun twice on one publisher, or ended with none open |
| `SchemaRegistry`, `SchemaRegistryTimeout` | a registry request failed or went unanswered (`schema-registry` feature) |
| `WireFormat` | a value did not match its Avro or Protobuf wire form (`schema-registry` feature) |

A service meets the error in two places.
A failed startup carries it as the source of the runtime's error, and the service exits before it reads anything.
A publish from a handler returns it inside the core's `PublishError`, and the handler decides how the delivery settles.
The enum is `#[non_exhaustive]`, so a `match` on it ends with a wildcard arm.

## Example

A shipment the cluster refuses is logged with its cause, and the order is settled as a retry.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_broker_errors.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_broker_errors.rs#handler"
```

</TabItem>
</Tabs>
