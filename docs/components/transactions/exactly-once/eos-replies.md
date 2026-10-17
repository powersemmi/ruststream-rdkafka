---
sidebar_position: 1
title: EosReplies
description: "EosReplies joins every reply to the pipeline's open window, paired with its delivery's consumed offset."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# EosReplies

[`EosReplies`](api:ruststream_rdkafka::EosReplies) joins every reply to the pipeline's open window, paired with its delivery's consumed offset.

It is a publish transform for the reply position, written after the policy: `.out_reply(EosPublish::new(id)).transform(EosReplies)`.
A `Commit::Transactional` subscription marks each delivery with its source coordinates, and the transform carries them onto the reply.
The pipeline reads them, pairs the reply with that offset, and drops them before the record is produced, so they never reach the topic.
The handler only returns its value: it names no publisher and no offset.
Without the transform, a reply carries no coordinates and its publish fails with [`KafkaError::InvalidOptions`](api:ruststream_rdkafka::KafkaError::InvalidOptions), so nothing leaves outside the window.
A reply from a subscription in another commit mode fails the same way.

A handler that settles without a reply, a drop or an `Err(outcome)`, publishes nothing, and its offset still commits with the window.

## Example

A payment is booked on a pool of four lanes; a payment of zero is dropped.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_eos_replies.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_eos_replies.rs#handler"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_eos_replies.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_eos_replies.rs#app"
```

</TabItem>
</Tabs>
