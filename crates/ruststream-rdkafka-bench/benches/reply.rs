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
//! Replying: the handler returns a value, the runtime encodes it and hands it to this crate's
//! publisher, which produces it to the topic the reply type declares and awaits its delivery
//! report.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::prelude::*;
use ruststream_rdkafka::{KafkaTopic, StartOffset};
use serde::Serialize;

/// A reply with a destination of its own, `common::REPLIES`: the mount site adds nothing to it.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
}

#[subscriber(
    KafkaTopic::new(common::topic())
        .group(common::group())
        .start(StartOffset::Earliest),
    publish
)]
async fn confirm(order: &Order, ctx: &mut Context<'_, (), Latch>) -> Confirmation {
    ctx.state().arrived();
    Confirmation {
        id: black_box(order.id),
    }
}

fn app(messages: usize) -> Pending {
    common::pending(messages, |b| {
        b.include(confirm);
    })
}

// Three runs allocated exactly 16,364 blocks over the primer and 2,000 replies, 8 per message: the
// three of a consume, and five for the publish - the librdkafka header list, the delivery channel
// and its boxed sender, the topic name as a C string, and the librdkafka message. The floor is
// that count plus 0.1 percent, 16,382; one more allocation per message would reach 18,364.
#[library_benchmark(config = common::config_every(8_010, 1_000, 362))]
#[bench::first(app(0))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = reply_group; benchmarks = service);
main!(library_benchmark_groups = reply_group);
