---
sidebar_position: 2
title: EosPipeline
description: "EosWindow::publish_paired sends a record into the open window by hand, with the source coordinates the Source key gives."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# EosPipeline

[`EosWindow::publish_paired`](api:ruststream_rdkafka::EosWindow::publish_paired) sends a record into the open window by hand, with the source coordinates the `Source` key gives.

[`EosPipeline`](api:ruststream_rdkafka::EosPipeline) is the live form of [`EosPublish`](./index.md), so a slot bound to that policy holds one.
A handler writes into the window through the slot's [`EosWindow`](api:ruststream_rdkafka::EosWindow) capability, `Out<impl EosWindow>`, and the manual form names the slot's wired value.
The delivery's coordinates are a [`SourceOffset`](api:ruststream_rdkafka::SourceOffset), read with `Ctx<Source>` ([`context::keys::Source`](api:ruststream_rdkafka::context::keys::Source)).
`window.publish_paired(&source, record, None)` adds the record to the window and pairs it with that offset; the last argument is the per-call options, and `None` keeps the producer's own partition choice.
A handler publishes as many records as it needs, then acks: the settled delivery is what lets the window commit.
A failed publish poisons the window, which aborts and is delivered again, so the handler answers it with `retry()`.
The record is an [`OutgoingMessage`](api:ruststream::OutgoingMessage) the handler encodes itself.
A refused record comes back inside the error, so the handler still holds it.
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
