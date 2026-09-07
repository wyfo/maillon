use core::ptr;

#[cfg(feature = "atomic-wait")]
pub use super::atomic_wait::AtomicParker;
#[cfg(feature = "parking_lot")]
pub use super::parking_lot::ParkingLotParker;
#[cfg(all(feature = "pthread", unix))]
pub use super::pthread::PthreadParker;
pub use super::spin::SpinParker;
#[cfg(feature = "std")]
pub use super::std::StdParker;
use crate::sync::{condvar::CondVar, mutex::Mutex};

/// Default number of spins before parking, e.g. for [`Eager`](crate::list::Eager).
///
/// Zero under `miri` and `loom`: every spin is an instrumented atomic load, so spinning
/// multiplies the state space a model has to explore and the branch budget it consumes,
/// without exercising anything the park path does not already cover.
#[cfg(not(any(miri, loom)))]
pub const DEFAULT_SPIN_BEFORE_PARK: usize = 100; // same as `std::sys::sync::mutex::futex`
/// Default number of spins before parking, e.g. for [`Eager`](crate::list::Eager).
///
/// Zero under `miri` and `loom`: every spin is an instrumented atomic load, so spinning
/// multiplies the state space a model has to explore and the branch budget it consumes,
/// without exercising anything the park path does not already cover.
#[cfg(any(miri, loom))]
pub const DEFAULT_SPIN_BEFORE_PARK: usize = 0;

// TODO it must not have spurious wakeup, can use notified in a loop
// TODO safety: `park_until` must not unwind, a panic in `Node`/`Drain` drop cannot be recovered
/// # Safety
///
/// TODO
pub unsafe trait Parker: Send + Sync + 'static {
    const NEVER_BLOCKS: bool = false;
    const INIT: Self;
    #[doc(hidden)]
    fn new() -> Self
    where
        Self: Sized,
    {
        Self::INIT
    }
    fn parked_state(&self) -> *mut () {
        ptr::null_mut()
    }
    /// # Safety
    ///
    /// `park_until` can only be called by a single thread at a time.
    /// `notified` must not panic.
    unsafe fn park_until<T>(&self, notified: impl FnMut() -> Option<T>) -> T;
    /// # Safety
    ///
    /// `parked_state` argument must have been returned by a `parked_state` call
    /// preceding a `park_until` call.
    unsafe fn unpark(&self, parked_state: *mut ());
}

/// TODO
/// A [`Parker`] built from a [`Mutex`] and a [`CondVar`], for platforms which have no native
/// parking primitive.
///
/// Platforms which do have one — futex-like APIs, FreeRTOS task notifications, bare-metal
/// `WFE`/`SEV` — should implement [`Parker`] directly instead: they express it without a
/// mutex, without a condition variable, and often without any state at all.
// implementation inspired from the std parker futex/pthread implementations
#[derive(Debug)]
pub struct CondVarParker<M: Mutex, C: CondVar<M>, const NOTIFY_WITH_MUTEX_ACQUIRED: bool> {
    mutex: M,
    condvar: C,
}

unsafe impl<M: Mutex, C: CondVar<M>, const NOTIFY_WITH_MUTEX_ACQUIRED: bool> Parker
    for CondVarParker<M, C, NOTIFY_WITH_MUTEX_ACQUIRED>
{
    #[cfg(not(loom))]
    #[allow(clippy::declare_interior_mutable_const)]
    const INIT: Self = Self {
        mutex: M::INIT,
        condvar: C::INIT,
    };
    #[cfg(loom)]
    const INIT: Self = unimplemented!();
    fn new() -> Self {
        Self {
            mutex: M::new(),
            condvar: C::new(),
        }
    }

    #[inline]
    unsafe fn park_until<T>(&self, mut notified: impl FnMut() -> Option<T>) -> T {
        let mut guard = self.mutex.lock();
        loop {
            if let Some(res) = notified() {
                // SAFETY: the guard comes from `self.mutex`, and is used only once.
                unsafe { self.mutex.unlock(guard) };
                return res;
            }
            // SAFETY: the guard comes from `self.mutex`, the only mutex used with
            // `self.condvar`.
            guard = unsafe { self.condvar.wait(&self.mutex, guard) };
        }
    }

    #[inline]
    unsafe fn unpark(&self, _parked_state: *mut ()) {
        // Acquiring the mutex waits for the parked thread to be actually waiting on the
        // condition variable, so the notification cannot be missed. There is a single
        // parked thread by contract, hence no other one can register in between.
        // SAFETY: the guard comes from this very mutex, and is used only once.
        let lock = self.mutex.lock();
        if NOTIFY_WITH_MUTEX_ACQUIRED {
            self.condvar.notify_one();
        }
        unsafe { self.mutex.unlock(lock) };
        if !NOTIFY_WITH_MUTEX_ACQUIRED {
            self.condvar.notify_one();
        }
    }
}

cfg_if::cfg_if! {
    if #[cfg(loom)] {
      pub type DefaultParker = StdParker;
    } else if #[cfg(feature = "atomic-wait")] {
        pub type DefaultParker = AtomicParker;
    } else if #[cfg(feature = "parking_lot")] {
        pub type DefaultParker = ParkingLotParker;
    } else if #[cfg(feature = "std")] {
        pub type DefaultParker = StdParker;
    } else if #[cfg(all(feature = "pthread", unix))] {
        pub type DefaultParker = PthreadParker;
    } else {
        pub type DefaultParker = SpinParker;
    }
}
