//! The batch stream the subscriber hands the runtime: an async step that collects a batch into a
//! buffer of its own, and a slot the runtime lends.
//!
//! A batch waits for the cluster and, with the schema registry, for the registry, so it is
//! collected by an async step rather than inside one poll. The step cannot hold the lent slot
//! across its awaits, so it fills a buffer the stream owns, and a finished batch trades places
//! with the slot: the runtime gets the batch, the stream keeps the emptied slot for the next one.
//! The two buffers alternate, so after the first batch none is allocated.

use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;
use pin_project_lite::pin_project;
use ruststream::BatchStream;

/// What one step of a [`Refill`] ends with: the batch's outcome (`None` when the subscription
/// ended), the state for the next step, and the buffer, holding the batch on `Some(Ok(()))` and
/// empty otherwise.
pub(crate) type Filled<State, Message, Error> = (Option<Result<(), Error>>, State, Vec<Message>);

pin_project! {
    /// A [`BatchStream`] over an async step that collects each batch into the stream's own
    /// buffer.
    pub(crate) struct Refill<State, Message, Step, Fut> {
        // The state and the buffer between steps; `None` while a step runs and after the end.
        idle: Option<(State, Vec<Message>)>,
        step: Step,
        #[pin]
        running: Option<Fut>,
    }
}

impl<State, Message, Step, Fut> Refill<State, Message, Step, Fut> {
    /// A stream whose first step starts from `state` and collects into `buffer`.
    pub(crate) const fn new<Error>(state: State, buffer: Vec<Message>, step: Step) -> Self
    where
        Step: FnMut(State, Vec<Message>) -> Fut,
        Fut: Future<Output = Filled<State, Message, Error>>,
    {
        Self {
            idle: Some((state, buffer)),
            step,
            running: None,
        }
    }
}

impl<State, Message, Step, Fut, Error> BatchStream<Vec<Message>> for Refill<State, Message, Step, Fut>
where
    Step: FnMut(State, Vec<Message>) -> Fut,
    Fut: Future<Output = Filled<State, Message, Error>>,
{
    type Error = Error;

    fn poll_fill(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        slot: &mut Vec<Message>,
    ) -> Poll<Option<Result<(), Error>>> {
        let mut this = self.project();
        if this.running.is_none() {
            let Some((state, buffer)) = this.idle.take() else {
                return Poll::Ready(None);
            };
            this.running.set(Some((this.step)(state, buffer)));
        }
        let Some(running) = this.running.as_mut().as_pin_mut() else {
            unreachable!("a step was started above");
        };
        let (outcome, state, mut buffer) = ready!(running.poll(cx));
        this.running.set(None);
        if matches!(outcome, Some(Ok(()))) {
            // The batch goes to the runtime in place of the slot it lent, and the emptied slot
            // becomes the buffer of the next step.
            mem::swap(slot, &mut buffer);
        }
        if outcome.is_some() {
            *this.idle = Some((state, buffer));
        }
        Poll::Ready(outcome)
    }
}

#[cfg(feature = "testing")]
pin_project! {
    /// The batch stream of one of the subscriber's two transports: librdkafka's consumer, or the
    /// member of the in-process cluster a test connects.
    #[project = ArmProjection]
    pub(crate) enum Arm<Live, InProcess> {
        Live { #[pin] stream: Live },
        InProcess { #[pin] stream: InProcess },
    }
}

#[cfg(feature = "testing")]
impl<Slot, Live, InProcess> BatchStream<Slot> for Arm<Live, InProcess>
where
    Live: BatchStream<Slot>,
    InProcess: BatchStream<Slot, Error = Live::Error>,
{
    type Error = Live::Error;

    fn poll_fill(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        slot: &mut Slot,
    ) -> Poll<Option<Result<(), Self::Error>>> {
        match self.project() {
            ArmProjection::Live { stream } => stream.poll_fill(cx, slot),
            ArmProjection::InProcess { stream } => stream.poll_fill(cx, slot),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Ready, ready};
    use std::pin::pin;
    use std::task::Waker;

    use super::*;

    /// A step that collects `per_batch` numbers counting up from the state, and ends once it
    /// reached `end`.
    fn counting(
        per_batch: u32,
        end: u32,
    ) -> impl FnMut(u32, Vec<u32>) -> Ready<Filled<u32, u32, &'static str>> {
        move |next, mut buffer| {
            if next >= end {
                return ready((None, next, buffer));
            }
            buffer.extend(next..next + per_batch);
            ready((Some(Ok(())), next + per_batch, buffer))
        }
    }

    fn fill<Stream: BatchStream<Vec<u32>>>(
        stream: Pin<&mut Stream>,
        slot: &mut Vec<u32>,
    ) -> Poll<Option<Result<(), Stream::Error>>> {
        stream.poll_fill(&mut Context::from_waker(Waker::noop()), slot)
    }

    #[test]
    fn each_fill_hands_the_runtime_the_next_batch() {
        let mut stream = pin!(Refill::new(0, Vec::with_capacity(3), counting(3, 6)));
        let mut slot = Vec::with_capacity(3);

        assert_eq!(fill(stream.as_mut(), &mut slot), Poll::Ready(Some(Ok(()))));
        assert_eq!(slot, [0, 1, 2]);
        slot.clear();
        assert_eq!(fill(stream.as_mut(), &mut slot), Poll::Ready(Some(Ok(()))));
        assert_eq!(slot, [3, 4, 5]);
    }

    #[test]
    fn the_buffers_alternate_so_a_batch_allocates_nothing() {
        let mut stream = pin!(Refill::new(0, Vec::with_capacity(4), counting(4, 16)));
        let mut slot = Vec::with_capacity(4);
        let mut storage = Vec::new();

        for _ in 0..4 {
            assert_eq!(fill(stream.as_mut(), &mut slot), Poll::Ready(Some(Ok(()))));
            storage.push(slot.as_ptr());
            slot.clear();
        }

        // Two buffers, taking turns: the stream's own and the one the runtime lent.
        assert_eq!(storage[0], storage[2]);
        assert_eq!(storage[1], storage[3]);
        assert_ne!(storage[0], storage[1]);
    }

    #[test]
    fn the_end_leaves_the_slot_empty_and_stays_the_end() {
        let mut stream = pin!(Refill::new(0, Vec::new(), counting(2, 2)));
        let mut slot = Vec::new();

        assert_eq!(fill(stream.as_mut(), &mut slot), Poll::Ready(Some(Ok(()))));
        slot.clear();
        assert_eq!(fill(stream.as_mut(), &mut slot), Poll::Ready(None));
        assert!(slot.is_empty());
        assert_eq!(fill(stream.as_mut(), &mut slot), Poll::Ready(None));
    }

    #[test]
    fn a_failed_fill_leaves_the_slot_empty_and_the_next_fill_starts_a_batch() {
        let mut failed_once = false;
        let step = move |next: u32, mut buffer: Vec<u32>| {
            if !failed_once {
                failed_once = true;
                return ready((Some(Err("fetch failed")), next, buffer));
            }
            buffer.push(next);
            ready((Some(Ok(())), next + 1, buffer))
        };
        let mut stream = pin!(Refill::new(7, Vec::new(), step));
        let mut slot = Vec::new();

        assert_eq!(fill(stream.as_mut(), &mut slot), Poll::Ready(Some(Err("fetch failed"))));
        assert!(slot.is_empty());
        assert_eq!(fill(stream.as_mut(), &mut slot), Poll::Ready(Some(Ok(()))));
        assert_eq!(slot, [7]);
    }
}
