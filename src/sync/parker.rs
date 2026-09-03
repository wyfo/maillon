#[cfg(feature = "atomic-wait")]
pub use super::atomic_wait::AtomicParker;
#[cfg(feature = "parking_lot")]
pub use super::parking_lot::ParkingLotParker;
#[cfg(all(feature = "pthread", unix))]
pub use super::pthread::PthreadParker;
pub use super::spin::SpinParker;
#[cfg(feature = "std")]
pub use super::std::StdParker;
use crate::{
    loom::sync::atomic::{AtomicUsize, Ordering::*},
    sync::{condvar::CondVar, mutex::Mutex},
};

pub trait Parker: Send + Sync {
    const NEVER_BLOCKS: bool = false;
    const INIT: Self;
    #[doc(hidden)]
    fn new() -> Self
    where
        Self: Sized,
    {
        Self::INIT
    }
    /// # Safety
    ///
    /// `park` can only be called by a single thread at a time.
    unsafe fn park(&self);
    fn unpark(&self);
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
pub struct CondVarParker<M: Mutex, C: CondVar<M>> {
    state: AtomicUsize,
    mutex: M,
    condvar: C,
}

impl<M: Mutex, C: CondVar<M>> CondVarParker<M, C> {
    const EMPTY: usize = 0;
    const NOTIFIED: usize = 1;
    const PARKED: usize = usize::MAX;
}

impl<M: Mutex, C: CondVar<M>> Parker for CondVarParker<M, C> {
    #[cfg(not(loom))]
    #[allow(clippy::declare_interior_mutable_const)]
    const INIT: Self = Self {
        state: AtomicUsize::new(Self::EMPTY),
        mutex: M::INIT,
        condvar: C::INIT,
    };
    #[cfg(loom)]
    const INIT: Self = unimplemented!();
    fn new() -> Self {
        Self {
            state: AtomicUsize::new(Self::EMPTY),
            mutex: M::new(),
            condvar: C::new(),
        }
    }

    #[inline]
    unsafe fn park(&self) {
        if self.state.fetch_sub(1, Acquire) == Self::NOTIFIED {
            return;
        }
        let mut guard = self.mutex.lock();
        while (self.state)
            .compare_exchange(Self::NOTIFIED, Self::EMPTY, Acquire, Relaxed)
            .is_err()
        {
            // SAFETY: the guard comes from `self.mutex`, the only mutex used with
            // `self.condvar`.
            guard = unsafe { self.condvar.wait(&self.mutex, guard) };
        }
        // SAFETY: the guard comes from `self.mutex`, and is used only once.
        unsafe { self.mutex.unlock(guard) };
    }

    #[inline]
    fn unpark(&self) {
        if self.state.swap(Self::NOTIFIED, Release) == Self::PARKED {
            // Acquiring the mutex waits for the parked thread to be actually waiting on the
            // condition variable, so the notification cannot be missed. There is a single
            // parked thread by contract, hence no other one can register in between.
            // SAFETY: the guard comes from this very mutex, and is used only once.
            unsafe { self.mutex.unlock(self.mutex.lock()) };
            self.condvar.notify_all();
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
