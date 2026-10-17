---
sidebar_position: 2
title: EosPipeline
description: "EosPipeline::publish sends a record into the open window by hand, with the source coordinates the Source key gives."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# EosPipeline

[`EosPipeline::publish`](api:ruststream_rdkafka::EosPipeline::publish) sends a record into the open window by hand, with the source coordinates the `Source` key gives.

[`EosPipeline`](api:ruststream_rdkafka::EosPipeline) is the live form of [`EosPublish`](./index.md), so a slot bound to that policy holds one.
A handler reaches it through the slot's `Borrow<EosPipeline>` bound, `Out<impl Borrow<EosPipeline>>`, and the manual form names it as the slot's wired value.
The delivery's coordinates are a [`SourceOffset`](api:ruststream_rdkafka::SourceOffset), read with `Ctx<Source>` ([`context::keys::Source`](api:ruststream_rdkafka::context::keys::Source)).
`publish(&source, record, options)` adds the record to the window and pairs it with that offset; `None` keeps the producer's own partition choice.
A handler publishes as many records as it needs, then acks: the settled delivery is what lets the window commit.
A failed publish poisons the window, which aborts and is delivered again, so the handler answers it with `retry()`.
The record is an [`OutgoingMessage`](api:ruststream::OutgoingMessage) the handler encodes itself.
[`id`](api:ruststream_rdkafka::EosPipeline::id) returns the pipeline id.

## Example

A cart is split into one record per item, and every record commits with the cart's offset.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_eos_pipeline.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_eos_pipeline.rs#handler"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_eos_pipeline.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_eos_pipeline.rs#app"
```

</TabItem>
</Tabs>
