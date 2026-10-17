---
sidebar_position: 1
title: Auto
description: "Commit::Auto is librdkafka's timer commit: an ack is advisory, and a retry is not delivered again."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Auto

[`Commit::Auto`](api:ruststream_rdkafka::Commit::Auto) is librdkafka's timer commit: an ack is advisory, and a retry is not delivered again.

It is the default of every descriptor, and librdkafka's own default.
librdkafka stores a record's position as it hands the record to the subscription, and commits the stored position every `auto.commit.interval.ms`.
The handler's outcome changes nothing about that position: an ack and a drop are advisory.
A [`retry`](api:ruststream::runtime::HandlerOutcome::retry) reports itself unsupported, and the delivery does not come back on the subscription or on the group's next member.
A crash loses the processed records whose position was not committed yet, and skips the stored records the handler had not finished.
This suits a consumer that can lose a few records, such as a dashboard or a cache.
A consumer that must process every record takes [`Commit::Tracked`](./tracked.md).

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_settlement_commit.rs#auto"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_settlement_commit.rs#auto"
```

</TabItem>
</Tabs>
