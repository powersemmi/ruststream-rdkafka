---
sidebar_position: 2
title: Security
description: "SASL and TLS are librdkafka properties set through config; the ssl and ssl-vendored features link the OpenSSL they need."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Security

SASL and TLS are librdkafka properties set through [`config`](./config.md); the `ssl` and `ssl-vendored` features link the OpenSSL they need.

`security.protocol` picks the listener: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT` or `SASL_SSL`.
`sasl.mechanism`, `sasl.username` and `sasl.password` set the SASL login, and the `ssl.*` properties the certificates.
The bootstrap servers are the addresses of the listener the protocol speaks to.

On the default features, librdkafka carries one SASL mechanism, `PLAIN`.
The `ssl` feature links the system OpenSSL, and `ssl-vendored` builds OpenSSL from source; either one adds TLS and the `SCRAM-SHA-256`, `SCRAM-SHA-512` and `OAUTHBEARER` mechanisms.
Without it, a TLS property fails startup with "OpenSSL not available at build time", and a SCRAM or OAUTHBEARER mechanism fails the connect.
The `zstd` feature adds that compression codec; the other codecs are built in.

A misspelled `security.protocol` fails startup, in a [`TestApp`](../testing/index.md) test as on a cluster.
The generated [AsyncAPI document](../asyncapi/server.md) reports the bootstrap addresses and never the credentials.

## Example

The dependency line:

```toml
ruststream-rdkafka = { version = "0.7", features = ["ssl"] }
```

The service authenticates with SCRAM over TLS and verifies the cluster against its own certificate authority.
The password comes from the environment.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_broker_security.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_broker_security.rs#broker"
```

</TabItem>
</Tabs>
