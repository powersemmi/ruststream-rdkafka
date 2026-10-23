//! The test binary's allocator: the system allocator, watched per thread.
//!
//! Each thread counts its own allocations and keeps the largest block it asked for, so a test
//! measures what runs on its own thread: neither the client's threads nor the tests running beside
//! it reach the numbers it reads.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    /// The allocations this thread has made.
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    /// The largest single block this thread has asked for since the last [`mark`].
    static LARGEST: Cell<usize> = const { Cell::new(0) };
}

struct Watching;

unsafe impl GlobalAlloc for Watching {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.with(|count| count.set(count.get() + 1));
        LARGEST.with(|largest| {
            if layout.size() > largest.get() {
                largest.set(layout.size());
            }
        });
        // SAFETY: the layout is the caller's, forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the pointer and layout are the caller's, forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Watching = Watching;

/// What this thread has allocated so far.
pub(crate) fn allocations() -> usize {
    ALLOCATIONS.with(Cell::get)
}

/// Forgets the blocks this thread asked for before the window under test.
pub(crate) fn mark() {
    LARGEST.with(|largest| largest.set(0));
}

/// The largest block this thread asked for since [`mark`].
pub(crate) fn largest() -> usize {
    LARGEST.with(Cell::get)
}
