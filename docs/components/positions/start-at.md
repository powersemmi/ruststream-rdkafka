---
sidebar_position: 1
title: start_at
description: "start_at opens a subscription at a position on every startup, whatever the group committed."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# start_at

`start_at` opens a subscription at a [position](./index.md) on every startup, whatever the group committed.

It is a clause of the attribute, `start_at(KafkaPosition::earliest())`, or [`.start_at(position)`](api:ruststream::runtime::SubscriberSettings::start_at) among the mount-site settings.
The position lands on the partitions the subscription opened with, ahead of their first record.
A subscription that opened with no partitions takes the position on the first assignment its group hands over.
Under `Assignment::CooperativeSticky` that first assignment may be part of the group's share, and partitions that arrive in a later increment start from the committed offset.
A position that names none of the partitions the subscription opened with stops the service at startup.
It suits a read model rebuilt from the retained log on each boot.
A group's [start offset](../subscriptions/start-offset.md) is different: it applies only where the group has committed nothing.

## Example

The audit projection is rebuilt from the oldest retained entry on every startup, although its group committed everything it read before.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_seek.rs#start_at"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_seek.rs#start_at"
```

</TabItem>
</Tabs>
