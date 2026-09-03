//! What the crate prelude puts in scope, checked at compile time.
//!
//! The one glob a Kafka service writes brings in the core prelude plus this crate's broker
//! vocabulary. Nothing here runs; the file exists so the name resolution below is a build gate.

use ruststream_rdkafka::prelude::*;

/// The core's slot capability trait must survive the glob: this crate exports its policies under
/// their prefixed names (`KafkaPublish`, ...) precisely so an alias cannot shadow it, and a
/// shadowing re-export would fail here with `E0404: expected trait, found struct`.
fn _publish_is_the_core_trait<T: Publish>() {}

/// The policies keep their prefix, and the transitions between them stay reachable through the
/// glob alone.
fn _policies_are_prefixed() {
    let plain = KafkaPublish::default();
    let transactional: KafkaTransactionalPublish = plain.transactional_id("svc-1");
    let _partitioned: KafkaPartitionedPublish = transactional.per_partition();
    let _pipeline = KafkaEosPublish::new("svc-1");
}

/// The seek vocabulary a handler names: the position type, the handle key, and the trait whose
/// method the key's value is called through.
fn _seek_vocabulary_resolves<S: Seeker<Position = KafkaPosition>>(seeker: &S) {
    let _ = (seeker, SeekHandle, Position);
}
