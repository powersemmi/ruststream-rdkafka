// The harness macros generate the group module, its items and the paths between them, and a
// benchmark function takes its setup value by value because the harness owns the drop; the
// crate's lints are written for the library surface, not for generated benchmark scaffolding.
#![allow(
    missing_docs,
    unused_qualifications,
    unreachable_pub,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value
)]
//! Consuming a small JSON body: the consumer group's subscription yields a record librdkafka has
//! fetched, the dispatcher decodes it into a struct, the handler reads a field, and the runtime
//! acks it.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::prelude::*;
use ruststream_rdkafka::{KafkaTopic, StartOffset};

#[subscriber(
    KafkaTopic::new(common::topic())
        .group(common::group())
        .start(StartOffset::Earliest)
)]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Pending {
    common::pending(messages, |b| {
        b.include(consume);
    })
}

// Three runs allocated exactly 6,266 blocks over the primer and 2,000 deliveries, 3 per message: the
// client's `Arc` around each fetched event, and the two blocks librdkafka allocates when the
// headers of a record without any are read. The floor is that count plus 0.1 percent, 6,273; one
// more allocation per message would reach 8,266.
#[library_benchmark(config = common::config_every(3_004, 1_000, 265))]
#[bench::first(app(0))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = consume_group; benchmarks = service);
main!(library_benchmark_groups = consume_group);
