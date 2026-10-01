//! What a subscription's stream serves ahead of the fetch queue.
//!
//! Two things reach a subscription from outside its consume loop: a delivery a handler handed back
//! with `nack(true)`, and a start position a rebalance could not apply. Both are rare, and the
//! consume loop asks for them once per delivery, so the question is one flag read; the rest sits
//! behind a mutex that only the rare paths take.

use std::collections::VecDeque;
use std::fmt;
use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};

use tokio::sync::Notify;
use tokio::sync::futures::Notified;

use crate::error::KafkaError;
use crate::message::KafkaMessage;

/// The subscription's side channel: requeued deliveries and a start failure, with the flag the
/// consume loop reads and the wake a waiting stream selects on.
#[derive(Default)]
pub(crate) struct Attention {
    /// Whether `pending` holds anything. Written only under the `pending` lock, so the flag never
    /// says empty while something waits there.
    raised: AtomicBool,
    pending: Mutex<Pending>,
    /// Woken whenever something is left in `pending`, so a stream waiting on a quiet topic
    /// serves it.
    wake: Notify,
}

#[derive(Default)]
struct Pending {
    /// A start position the rebalance could not apply. Nothing can be returned from a librdkafka
    /// callback, and a subscription silently reading from somewhere other than the position it
    /// was opened at must not pass for a working one, so the failure waits here for the stream.
    start_failure: Option<KafkaError>,
    /// Deliveries handed back with `nack(true)`, in the order they were handed back.
    requeued: VecDeque<KafkaMessage>,
    /// Set when the subscription closes: a delivery handed back after that has no stream to come
    /// back on, and is released instead.
    closed: bool,
}

impl Pending {
    fn is_empty(&self) -> bool {
        self.start_failure.is_none() && self.requeued.is_empty()
    }
}

impl fmt::Debug for Attention {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Attention")
            .field("raised", &self.raised.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Attention {
    /// Whether anything waits for the stream. The one question the consume loop asks per
    /// delivery.
    pub(crate) fn raised(&self) -> bool {
        self.raised.load(Ordering::Acquire)
    }

    fn lock(&self) -> MutexGuard<'_, Pending> {
        self.pending.lock().expect("attention mutex poisoned")
    }

    /// Records a start position the rebalance could not apply, keeping the first one: it names
    /// what went wrong, and the ones behind it are the same assignment failing again.
    pub(crate) fn record_start_failure(&self, err: KafkaError) {
        let mut pending = self.lock();
        if pending.start_failure.is_none() {
            pending.start_failure = Some(err);
        }
        self.raised.store(true, Ordering::Release);
        drop(pending);
        self.wake.notify_waiters();
    }

    /// Takes the recorded start failure, for the stream to yield.
    pub(crate) fn take_start_failure(&self) -> Option<KafkaError> {
        if !self.raised() {
            return None;
        }
        let mut pending = self.lock();
        let taken = pending.start_failure.take();
        self.raised.store(!pending.is_empty(), Ordering::Release);
        taken
    }

    /// Hands a delivery back to its subscription, which delivers it again ahead of the fetch
    /// queue. Once the subscription has closed the delivery is released unsettled instead, and
    /// the group hands its record to whoever reads the partition next.
    pub(crate) fn requeue(&self, delivery: KafkaMessage) {
        let mut pending = self.lock();
        if pending.closed {
            drop(pending);
            drop(delivery);
            return;
        }
        pending.requeued.push_back(delivery);
        self.raised.store(true, Ordering::Release);
        drop(pending);
        self.wake.notify_waiters();
    }

    /// Takes the delivery handed back first, for the stream to deliver again.
    pub(crate) fn take_requeued(&self) -> Option<KafkaMessage> {
        if !self.raised() {
            return None;
        }
        let mut pending = self.lock();
        let taken = pending.requeued.pop_front();
        self.raised.store(!pending.is_empty(), Ordering::Release);
        taken
    }

    /// A waiter for the next thing left here. Create it BEFORE asking [`raised`](Self::raised),
    /// so one landing between the two is not missed.
    pub(crate) fn waiter(&self) -> Notified<'_> {
        self.wake.notified()
    }

    /// Closes the side channel with its subscription, releasing what it still holds.
    ///
    /// A held delivery keeps the subscription's consumer alive, and the consumer keeps this
    /// channel alive, so the deliveries are released here rather than left to a cycle.
    pub(crate) fn close(&self) {
        let mut pending = self.lock();
        pending.closed = true;
        let requeued = mem::take(&mut pending.requeued);
        self.raised
            .store(pending.start_failure.is_some(), Ordering::Release);
        drop(pending);
        // Dropped outside the lock: releasing a delivery may release the consumer with it.
        drop(requeued);
    }
}
