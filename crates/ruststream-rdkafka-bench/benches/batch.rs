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
//! Consuming in batches of 64: the subscription hands over one record and whatever librdkafka has
//! already fetched behind it, up to 64, the handler reads them as a slice, and the runtime settles
//! every delivery in it.

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
async fn consume(orders: &[Order], ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    for order in orders {
        black_box((order.id, order.quantity));
        ctx.state().arrived();
    }
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Pending {
    common::pending(messages, |b| {
        b.include(consume.batch(nonzero!(64)));
    })
}

// Three runs allocated exactly 6,299 blocks over the primer and 2,000 deliveries, 3 per message
// as in `consume`, and one per batch. The floor is that count plus 0.1 percent, 6,306; one more
// allocation per message would reach 8,299.
#[library_benchmark(config = common::config_every(3_020, 1_000, 266))]
#[bench::first(app(0))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = batch_group; benchmarks = service);
main!(library_benchmark_groups = batch_group);
