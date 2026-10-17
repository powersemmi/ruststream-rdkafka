---
sidebar_position: 4
title: Dead letters
description: "dead_letter names the topic a delivery that reached its cap is republished to, payload and headers as they arrived."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Dead letters

[`dead_letter(topic)`](api:ruststream::runtime::Mounting::dead_letter) names the topic a delivery that reached its cap is republished to, payload and headers as they arrived.

It follows [`max_attempts(n)`](api:ruststream::runtime::Mounting::max_attempts) at the mount site.
When a [retry](./retries.md) would make the delivery the `n`-th one, the runtime settles it and publishes it to the dead-letter topic instead.
The record keeps its payload, its headers and its record key.
[`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER) on it says how many times it was delivered.
A cap without a dead-letter topic rejects the spent delivery, which settles its offset, and a warning names the subscription.
The dead-letter topic is the service's own infrastructure, an ordinary topic: a subscription of the same service, or of another one, reads it.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_retries.rs#declaration"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_retries.rs#declaration"
```

</TabItem>
</Tabs>

The dead-letter topic is read by an ordinary subscription:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_retries.rs#dead_letter"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_retries.rs#dead_letter"
```

</TabItem>
</Tabs>
