//! What the crate prelude puts in scope, checked at compile time.
//!
//! The glob a mount site writes brings in the core prelude plus this crate's broker vocabulary.
//! Nothing here runs; the file exists so the name resolution below is a build gate.

use ruststream_rdkafka::prelude::*;

/// A handler bounds its injected slot with a capability trait, never a policy type, and the
/// traits reach a file through the glob as well.
fn _slots_are_bound_by_capability<T: Publisher>() {}

/// The policies arrive under their concept names, which is what lets an include site read the
/// same on every broker. These are the mount-site half of the vocabulary; a name here resolving
/// to anything but the policy - something of the same name arriving through the core glob above -
/// would fail this build rather than a service's.
fn _policies_carry_the_concept_names() {
    let plain: Publish = Publish::default();
    let transactional: TransactionalPublish = plain.transactional_id("svc-1");
    let _partitioned: PartitionedPublish = transactional.per_partition();
    let _pipeline = EosPublish::new("svc-1");
}

/// The seek vocabulary a handler names: the position type, the handle key, and the trait whose
/// method the key's value is called through.
fn _seek_vocabulary_resolves<S: Seeker<Position = KafkaPosition>>(seeker: &S) {
    let _ = (seeker, SeekHandle, Position);
}
