use core::marker::PhantomData;

use crate::{
    backoff::{BackoffStrategy, SpinBackoff},
    loom::sync::atomic::{AtomicBool, Ordering::*},
    sync::{mutex::Mutex, parker::Parker},
};

/// A spinning [`Mutex`] implementation.
///
/// While the mutex is locked by another thread, the lock spins using the backoff strategy `B`.
pub struct SpinMutex<B: BackoffStrategy = SpinBackoff>(AtomicBool, PhantomData<B>);

unsafe impl<B: BackoffStrategy> Mutex for SpinMutex<B> {
    #[cfg(not(loom))]
    const INIT: Self = Self(AtomicBool::new(false), PhantomData);
    #[cfg(loom)]
    const INIT: Self = unimplemented!();
    #[cfg(loom)]
    fn new() -> Self
    where
        Self: Sized,
    {
        Self(AtomicBool::new(false), PhantomData)
    }
    type Guard<'a>
        = ()
    where
        Self: 'a;
    #[inline]
    fn lock(&self) -> Self::Guard<'_> {
        while self.0.swap(true, Acquire) {
            B::default().backoff_until(|| !self.0.load(Relaxed));
        }
    }
    #[inline]
    unsafe fn unlock<'a>(&'a self, _guard: Self::Guard<'a>) {
        self.0.store(false, Release);
    }
}

/// A spinning [`Parker`] implementation.
///
/// The thread is never parked: `park_until` spins using the backoff strategy `B` until the
/// notification is received.
pub struct SpinParker<B: BackoffStrategy = SpinBackoff>(PhantomData<B>);

impl<B: BackoffStrategy> Parker for SpinParker<B> {
    const NEVER_BLOCKS: bool = true;
    const INIT: Self = Self(PhantomData);
    #[inline]
    unsafe fn park_until<T>(&self, notified: impl FnMut() -> Option<T>) -> T {
        B::default().backoff_until(notified)
    }
    #[inline]
    unsafe fn unpark(&self, _parked_state: *mut ()) {}
}
