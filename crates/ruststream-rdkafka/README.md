# ruststream-rdkafka

Apache Kafka broker implementation for the
[RustStream](https://github.com/powersemmi/ruststream) messaging framework, backed by
[rdkafka](https://docs.rs/rdkafka) / librdkafka: consumer groups as subscription descriptors,
auto or tracked commit modes, native record keys for per-key ordering, and raw librdkafka
config passthrough everywhere.

Guides and examples: <https://powersemmi.github.io/ruststream-rdkafka/>.

## Testing

The `testing` feature ships an in-process test broker (`KafkaTestBroker`) for application tests
with the core `TestApp` harness:

```toml
[dev-dependencies]
ruststream-rdkafka = { version = "0.6", features = ["testing"] }
```

Never enable this feature in production builds.
