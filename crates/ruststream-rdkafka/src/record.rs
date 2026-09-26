//! The native record a delivery owns.
//!
//! librdkafka hands a fetched record out as a [`BorrowedMessage`], whose lifetime ties it to the
//! consumer that polled it. The data behind it is not the consumer's: it belongs to the fetch
//! event the message carries a reference count of, and the lifetime exists for one reason only -
//! to order the record's destruction before `rd_kafka_destroy`. A delivery that holds the
//! consumer alive itself satisfies that order, which is what this type does: the record stays
//! where librdkafka put it for as long as a handler holds the delivery, and the body, the key
//! and the headers are read out of the fetch buffer instead of copied out of it.
//!
//! The pair is a [`Yoke`]. Its cart is one `Arc` over the consumer and the block every delivery
//! of the subscription shares (the commit tracker, the seeker, the topic name), so the one
//! reference count a delivery takes keeps all of them alive. The record is fetched against the
//! reference the cart hands out, so the borrow is the compiler's own and this crate writes no
//! `unsafe` of its own.
//!
//! A yoke is built by a synchronous closure, which is what the consume loop wants: a record
//! already fetched is taken without a future. The wait has no such builder, and a record taken
//! by a future borrows the caller's consumer rather than the cart, so the one delivery the wait
//! itself resolves is copied out instead of yoked (see [`HeldRecord::next`]).

use std::fmt;
use std::future::{Future as _, poll_fn};
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;

use futures::FutureExt as _;
use rdkafka::consumer::{Consumer as _, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{
    BorrowedHeaders, BorrowedMessage, Header, Headers as _, Message as _, OwnedHeaders,
    OwnedMessage,
};
use ruststream::Str;
use yoke::{Yoke, Yokeable};

#[cfg(feature = "testing")]
use crate::in_process::{InProcessRecord, WireHeader};
use crate::seek::KafkaSeeker;
use crate::tracker::{CommitTracker, TrackingContext};

/// The consumer a subscription reads through, shared with every delivery it produced.
pub(crate) type SharedConsumer = Arc<StreamConsumer<TrackingContext>>;

/// What every delivery of one subscription shares with it, behind one reference count: the
/// commit tracker a settle reports to, the seeker a context hands out, and the topic name a
/// delivery reads when it carries none of its own.
#[derive(Debug)]
pub(crate) struct Shared {
    pub(crate) tracker: Arc<CommitTracker>,
    pub(crate) seeker: Arc<KafkaSeeker>,
    /// The subscription's topic: its one topic, or, for a subscription over several names or a
    /// pattern, the name it was declared with.
    topic: Str,
}

impl Shared {
    pub(crate) const fn new(
        tracker: Arc<CommitTracker>,
        seeker: Arc<KafkaSeeker>,
        topic: Str,
    ) -> Self {
        Self {
            tracker,
            seeker,
            topic,
        }
    }

    /// The topic of a delivery that carries `own` as its own name.
    ///
    /// A delivery (and the context built off it) is one public type for every form of
    /// subscription, so whether it names its own topic is a field and not a type. The subscriber
    /// gives every record of a subscription over several topics its own name; a delivery without
    /// one belongs to a subscription over one topic, which is the name held here. The declared
    /// name a subscription over several topics holds here is never what its deliveries answer.
    pub(crate) fn topic<'a>(&'a self, own: Option<&'a Str>) -> &'a Str {
        own.unwrap_or(&self.topic)
    }
}

/// The cart a fetched record is yoked to: the consumer the record is borrowed from, and what the
/// deliveries share, both behind the one reference count the yoke already costs.
pub(crate) struct LiveShared {
    pub(crate) consumer: SharedConsumer,
    /// Counted apart from the cart, so a per-delivery context holds it without the consumer.
    pub(crate) shared: Arc<Shared>,
}

/// The cart, as a delivery holds it.
pub(crate) type Cart = Arc<LiveShared>;

/// The record, as the yoke carries it: a wrapper of this crate's own, because the yokeable is
/// the type the cart's borrow is proved covariant in and `rdkafka` does not implement the trait.
#[derive(Yokeable)]
pub(crate) struct Record<'a>(BorrowedMessage<'a>);

/// What a synchronous take of an already-fetched record can end in.
enum NotTaken {
    /// Nothing is fetched: the caller waits.
    Empty,
    /// The consumer reported an error instead of a record.
    Failed(KafkaError),
}

/// One fetched record, kept where librdkafka put it - or, for the record a wait resolved, the
/// copy that is the only owned form such a record can take.
pub(crate) enum HeldRecord {
    /// The record in librdkafka's own buffer, yoked to the cart that holds the consumer owning
    /// it.
    Fetched(Yoke<Record<'static>, Cart>),
    /// A record the wait took out of the queue itself. Its borrow is the waiter's, not the
    /// cart's, so it cannot enter a yoke; it is copied instead, which is what every delivery
    /// cost before this type existed. The copy is boxed because it is the wider arm by far, and
    /// an unboxed one sets the width of every delivery, including the overwhelming majority
    /// that are the other arm: it cost 224 instructions per delivery in `memcpy` alone, moving
    /// a copy that was not there.
    Copied {
        cart: Cart,
        message: Box<OwnedMessage>,
    },
    /// A record of the in-process cluster the test harness connected, behind the `testing`
    /// feature, with the block its subscription shares. Boxed like the copy, so the arm leaves
    /// the width of a delivery alone.
    #[cfg(feature = "testing")]
    InProcess {
        shared: Arc<Shared>,
        record: Box<InProcessRecord>,
    },
}

// The in-process arm exists only under `testing`: a production build keeps the two arms it had,
// and the delivery its width.
#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<HeldRecord>() == 3 * size_of::<usize>());

impl HeldRecord {
    /// A record librdkafka has already fetched, or `None` when the queue is empty.
    ///
    /// # Cancel safety
    ///
    /// Nothing is awaited: either a record is taken or nothing happens.
    pub(crate) fn ready(cart: &Cart) -> Option<Result<Self, KafkaError>> {
        let taken = Yoke::try_attach_to_cart(Arc::clone(cart), |cart| {
            cart.consumer
                .recv()
                .now_or_never()
                .ok_or(NotTaken::Empty)?
                .map(Record)
                .map_err(NotTaken::Failed)
        });
        match taken {
            Ok(yoke) => Some(Ok(Self::Fetched(yoke))),
            Err(NotTaken::Empty) => None,
            Err(NotTaken::Failed(err)) => Some(Err(err)),
        }
    }

    /// The next record, waiting for it.
    ///
    /// The wait is the cold path of the consume loop - a subscription keeping up with its topic
    /// finds the record already fetched - and it is the one place this shape costs something the
    /// hand-written one does not. `recv` is what registers a wakeup with the consumer's queue,
    /// and it registers it for the life of the `MessageStream` it opens: dropping the future
    /// takes the registration with it, so the waiter is kept across polls rather than armed and
    /// dropped. Every wake tries the yoke first, and the waiter is only consulted when the queue
    /// is empty again; a record the waiter itself resolves - the queue filled between the two
    /// steps - is borrowed from this call's consumer reference rather than from the cart, and is
    /// copied out, because nothing else can carry it past the end of this call.
    ///
    /// # Cancel safety
    ///
    /// Cancel safe: dropping this future before it resolves takes no record.
    pub(crate) async fn next(cart: &Cart) -> Result<Self, KafkaError> {
        let mut waiter = pin!(cart.consumer.recv());
        poll_fn(move |cx| {
            if let Some(taken) = Self::ready(cart) {
                return Poll::Ready(taken);
            }
            match waiter.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(message)) => Poll::Ready(Ok(Self::Copied {
                    cart: Arc::clone(cart),
                    message: Box::new(message.detach()),
                })),
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            }
        })
        .await
    }

    /// Stores `position` as this subscription's processed position on the record's partition,
    /// `offset` being the record's own.
    ///
    /// The position is the record's own offset whenever nothing below it is still outstanding,
    /// which is every settle of an in-order handler. There the record answers for its own topic
    /// and librdkafka takes the position off the record's topic handle, instead of looking one
    /// up by name (a `CString`, a topic create and a topic destroy under its own lock, per
    /// message).
    pub(crate) fn store(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        position: i64,
    ) -> Result<(), KafkaError> {
        match self {
            Self::Fetched(yoke) if position == offset => yoke
                .backing_cart()
                .consumer
                .store_offset_from_message(&yoke.get().0),
            Self::Fetched(yoke) => yoke
                .backing_cart()
                .consumer
                .store_offset(topic, partition, position),
            Self::Copied { cart, .. } => cart.consumer.store_offset(topic, partition, position),
            #[cfg(feature = "testing")]
            Self::InProcess { record, .. } => {
                record.store(topic, partition, position);
                Ok(())
            }
        }
    }

    /// What every delivery of the subscription shares.
    pub(crate) fn shared(&self) -> &Arc<Shared> {
        match self {
            Self::Fetched(yoke) => &yoke.backing_cart().shared,
            Self::Copied { cart, .. } => &cart.shared,
            #[cfg(feature = "testing")]
            Self::InProcess { shared, .. } => shared,
        }
    }

    /// The topic the record came from.
    pub(crate) fn topic(&self) -> &str {
        match self {
            Self::Fetched(yoke) => yoke.get().0.topic(),
            Self::Copied { message, .. } => message.topic(),
            #[cfg(feature = "testing")]
            Self::InProcess { record, .. } => record.topic(),
        }
    }

    /// The partition the record came from.
    pub(crate) fn partition(&self) -> i32 {
        match self {
            Self::Fetched(yoke) => yoke.get().0.partition(),
            Self::Copied { message, .. } => message.partition(),
            #[cfg(feature = "testing")]
            Self::InProcess { record, .. } => record.partition(),
        }
    }

    /// The record's offset in its partition.
    pub(crate) fn offset(&self) -> i64 {
        match self {
            Self::Fetched(yoke) => yoke.get().0.offset(),
            Self::Copied { message, .. } => message.offset(),
            #[cfg(feature = "testing")]
            Self::InProcess { record, .. } => record.offset(),
        }
    }

    /// The record's timestamp, when the broker or the producer set one.
    pub(crate) fn timestamp_millis(&self) -> Option<i64> {
        match self {
            Self::Fetched(yoke) => yoke.get().0.timestamp().to_millis(),
            Self::Copied { message, .. } => message.timestamp().to_millis(),
            #[cfg(feature = "testing")]
            Self::InProcess { record, .. } => Some(record.timestamp_millis()),
        }
    }

    /// The record's body, where librdkafka put it.
    pub(crate) fn payload(&self) -> &[u8] {
        match self {
            Self::Fetched(yoke) => yoke.get().0.payload().unwrap_or_default(),
            Self::Copied { message, .. } => message.payload().unwrap_or_default(),
            #[cfg(feature = "testing")]
            Self::InProcess { record, .. } => record.payload(),
        }
    }

    /// The record's key, where librdkafka put it.
    pub(crate) fn key(&self) -> Option<&[u8]> {
        match self {
            Self::Fetched(yoke) => yoke.get().0.key(),
            Self::Copied { message, .. } => message.key(),
            #[cfg(feature = "testing")]
            Self::InProcess { record, .. } => record.key(),
        }
    }

    /// The record's wire headers, when it carries any. Asked only when something reads them:
    /// librdkafka answers a record without headers by allocating a shadow buffer and freeing it
    /// again.
    pub(crate) fn headers(&self) -> Option<RecordHeaders<'_>> {
        match self {
            Self::Fetched(yoke) => yoke.get().0.headers().map(RecordHeaders::Fetched),
            Self::Copied { message, .. } => message.headers().map(RecordHeaders::Copied),
            #[cfg(feature = "testing")]
            Self::InProcess { record, .. } => Some(RecordHeaders::InProcess(record.headers())),
        }
    }
}

impl fmt::Debug for HeldRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HeldRecord")
            .field("partition", &self.partition())
            .field("offset", &self.offset())
            .finish_non_exhaustive()
    }
}

/// A record's wire headers, in whichever form the record itself is held. The two native header
/// types share a trait, but it carries a generic method and so cannot be asked for behind a
/// reference to the trait.
pub(crate) enum RecordHeaders<'a> {
    /// Headers read where librdkafka put them.
    Fetched(&'a BorrowedHeaders),
    /// Headers of a record the wait copied out.
    Copied(&'a OwnedHeaders),
    /// Headers of a record of the in-process cluster.
    #[cfg(feature = "testing")]
    InProcess(&'a [WireHeader]),
}

impl RecordHeaders<'_> {
    /// How many headers the record carries.
    fn count(&self) -> usize {
        match self {
            Self::Fetched(headers) => headers.count(),
            Self::Copied(headers) => headers.count(),
            #[cfg(feature = "testing")]
            Self::InProcess(headers) => headers.len(),
        }
    }

    /// The header at `index`, which the iteration below keeps in bounds.
    fn get(&self, index: usize) -> Header<'_, &[u8]> {
        match self {
            Self::Fetched(headers) => headers.get(index),
            Self::Copied(headers) => headers.get(index),
            #[cfg(feature = "testing")]
            Self::InProcess(headers) => {
                let (key, value) = &headers[index];
                Header {
                    key,
                    value: value.as_deref(),
                }
            }
        }
    }

    /// Iterates over all headers in order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = Header<'_, &[u8]>> {
        (0..self.count()).map(|index| self.get(index))
    }
}
