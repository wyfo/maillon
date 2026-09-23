//! The [`Parker`] abstraction and its implementations.
//!
//! [`CondVarParker`], a generic implementation based on [`CondVar`], is also provided.
use core::ptr;

#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "freebsd",
    target_os = "macos",
    target_os = "ios",
    target_os = "watchos",
    windows
))]
pub use super::atomic_wait::AtomicParker;
#[cfg(feature = "parking_lot")]
pub use super::parking_lot::ParkingLotParker;
#[cfg(all(feature = "pthread", unix))]
pub use super::pthread::PthreadParker;
pub use super::spin::SpinParker;
#[cfg(feature = "std")]
pub use super::std::StdParker;
use crate::sync::{condvar::CondVar, mutex::Mutex};

/// A thread parker abstraction.
pub trait Parker: Send + Sync + 'static {
    /// Whether the parker actually ever parks the thread or not.
    ///
    /// Spin loop based parkers never block, so they don't require synchronization with unparking.
    const NEVER_BLOCKS: bool = false;

    /// Initial parker value.
    const INIT: Self;

    #[doc(hidden)]
    fn new() -> Self
    where
        Self: Sized,
    {
        Self::INIT
    }

    /// A 2-aligned pointer retrieved before parking and passed later to `unpark`.
    ///
    /// It can be used to identify the parked thread when unparking it.
    fn parked_state(&self) -> *mut () {
        ptr::null_mut()
    }

    /// Parks the thread until a notification is received after a call to `unpark`.
    ///
    /// # Safety
    ///
    /// `park_until` can only be called by a single thread at a time.
    /// `notified` must not panic.
    unsafe fn park_until<T>(&self, notified: impl FnMut() -> Option<T>) -> T;

    /// Unparks a parked thread with its `parked_state`.
    ///
    /// # Safety
    ///
    /// `parked_state` argument must have been returned by a `parked_state` call preceding a
    /// `park_until` call.
    unsafe fn unpark(&self, parked_state: *mut ());
}

/// A [`Parker`] built from a [`Mutex`] and a [`CondVar`], for platforms which have no native
/// parking primitive.
///
/// If `NOTIFY_WITH_MUTEX_ACQUIRED` is `true`, then `CondVar::notify_one` will be called with the
/// mutex acquired.
#[derive(Debug)]
pub struct CondVarParker<M: Mutex, C: CondVar<M>, const NOTIFY_WITH_MUTEX_ACQUIRED: bool> {
    mutex: M,
    condvar: C,
}

impl<M: Mutex, C: CondVar<M>, const NOTIFY_WITH_MUTEX_ACQUIRED: bool> Parker
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
    if #[cfg(any(loom, miri))] {
        type DefaultParkerImpl = StdParker;
    } else if #[cfg(any(target_os = "linux", target_os = "android", target_os = "freebsd", target_os = "macos", target_os = "ios", target_os = "watchos", windows))] {
        type DefaultParkerImpl = AtomicParker;
    } else if #[cfg(feature = "parking_lot")] {
        type DefaultParkerImpl = ParkingLotParker;
    } else if #[cfg(feature = "std")] {
        type DefaultParkerImpl = StdParker;
    } else if #[cfg(all(feature = "pthread", unix))] {
        type DefaultParkerImpl = PthreadParker;
    } else {
        type DefaultParkerImpl = SpinParker;
    }
}

/// The default parker implementation used by [`AtomicEager`](crate::linking::AtomicEager).
///
/// If supported by the platform, [`AtomicWaiter`] is used. Otherwise, it is selected from the
/// enabled features, by decreasing priority: `parking_lot` (`ParkingLotParker`), `std`
/// (`StdParker`), `pthread` (`PthreadParker`, unix only), and [`SpinParker`] otherwise.
pub type DefaultParker = DefaultParkerImpl;
