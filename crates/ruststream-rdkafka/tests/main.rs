//! The integration suite, one test binary: each file under `tests/` is a module of it.

mod alloc_watch;
mod live;

mod asyncapi;
mod avro_transcode;
mod both_modes;
mod codec_registry;
mod conformance_rdkafka;
mod context_cost;
mod harness;
mod harness_manual;
mod harness_positions;
mod harness_transactions;
mod in_process;
mod integration_rdkafka;
mod live_delivery_cost;
mod live_held_delivery;
mod live_publish_cost;
mod live_shared_group;
mod live_subscription_start;
mod prelude;
mod protobuf_lanes;
mod protobuf_transcode;
mod publish_headers;
mod schema_registry;
mod secrets;
