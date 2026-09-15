#![doc = include_str!("README.md")]
#![forbid(unsafe_code)]

#[cfg(feature = "asyncapi")]
mod bindings;
mod broker;
mod convert;
mod distribution;
mod eos;
mod error;
mod message;
mod publisher;
mod redelivery;
mod seek;
mod subscriber;
mod subscription;
mod tracker;

#[cfg(feature = "avro")]
pub mod avro;
pub mod context;
pub mod prelude;
#[cfg(feature = "protobuf")]
pub mod protobuf;
#[cfg(feature = "schema-registry")]
pub mod schema_registry;
#[cfg(feature = "testing")]
pub mod testing;

pub use broker::{ClosedKafkaBroker, ConnectedKafkaBroker, KafkaBroker};
pub use distribution::RoundRobin;
pub use eos::{EOS_SOURCE_HEADER, EosPipeline, EosReplies, KafkaEosPublish, SourceOffset};
pub use error::KafkaError;
pub use message::{KafkaMessage, PARTITION_KEY_HEADER};
#[cfg(feature = "protobuf")]
pub use protobuf::{KafkaFramedPublish, KafkaFramedPublisher, ProtobufFrame};
pub use publisher::{
    KafkaOptions, KafkaPartitionedPublish, KafkaPublish, KafkaPublishSteps, KafkaPublisher,
    KafkaTransactionalPublish, KafkaTransactionalPublisher, PartitionLanes,
    TransactionalPartitions,
};
pub use redelivery::ToSourceTopic;
#[cfg(feature = "schema-registry")]
pub use schema_registry::{
    HttpRegistryClient, MemorySchemaCache, MissingSubject, RegisteredSchema, RegistryClient,
    SchemaCache, SchemaCachePolicy, SchemaFrame, SchemaFramed, SchemaPrefetch, SchemaRegistry,
    SchemaType, SubjectStrategy,
};
pub use seek::{KafkaPosition, KafkaSeeker};

pub use subscriber::KafkaSubscriber;
pub use subscription::{
    Assignment, Commit, KafkaPartitions, KafkaTopic, KafkaTopics, LaneKey, StartOffset,
};
