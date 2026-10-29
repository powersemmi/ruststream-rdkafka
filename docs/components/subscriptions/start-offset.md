---
sidebar_position: 6
title: StartOffset
description: "StartOffset says where a group starts on a partition with no committed offset: committed reset, earliest or latest."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# StartOffset

[`StartOffset`](api:ruststream_rdkafka::StartOffset) says where a group starts on a partition with no committed offset: committed reset, earliest or latest.

A group resumes from its committed offset whenever it has a valid one.
`StartOffset` applies only where it has none: before the group's first commit on a partition, and after retention deleted the record a committed offset pointed at.
The setting is librdkafka's `auto.offset.reset`, written on the descriptor with `start`.
`StartOffset::Committed`, the default, leaves the choice to librdkafka, which resets to the end of the log.
`StartOffset::Earliest` starts at the oldest record the topic retains.
`StartOffset::Latest` starts at the end of the log: only records published after the subscription opened arrive.
The second case matters for a group that sat idle longer than the topic's retention: with the default it skips to the end instead of reading what is left.
A position that applies on every startup, whatever the group committed, is [`start_at`](../positions/start-at.md).

## Example

A new archive group reads the invoices the topic held before the group existed.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_subscriptions_start_offset.rs#start"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_subscriptions_start_offset.rs#start"
```

</TabItem>
</Tabs>
