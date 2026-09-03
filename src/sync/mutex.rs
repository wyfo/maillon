#[cfg(all(feature = "pthread", unix))]
pub use super::pthread::PthreadMutex;
pub use super::spin::SpinMutex;
#[cfg(feature = "std")]
pub use super::std::StdMutex;

/// # Safety
///
/// Implementations of this trait must ensure that the mutex is actually
/// exclusive: a lock can't be acquired while the mutex is already locked.
///
/// Calls to [`unlock`](Self::unlock) must *synchronize-with* calls to [`lock`](Self::lock).
pub unsafe trait Mutex: Send + Sync {
    const INIT: Self;
    #[doc(hidden)]
    fn new() -> Self
    where
        Self: Sized,
    {
        Self::INIT
    }
    type Guard<'a>
    where
        Self: 'a;
    fn lock(&self) -> Self::Guard<'_>;
    /// # Safety
    ///
    /// The guard must have been returned from [`lock`](Self::lock), and must be used only once.
    unsafe fn unlock<'a>(&'a self, guard: Self::Guard<'a>);
}

#[cfg(feature = "lock_api")]
unsafe impl<R: lock_api::RawMutex + Send + Sync> Mutex for lock_api::Mutex<R, ()> {
    #[allow(clippy::declare_interior_mutable_const)]
    const INIT: Self = lock_api::Mutex::new(());
    type Guard<'a>
        = ()
    where
        Self: 'a;

    #[inline]
    fn lock(&self) -> Self::Guard<'_> {
        core::mem::forget(lock_api::Mutex::lock(self));
    }
    #[inline]
    unsafe fn unlock<'a>(&'a self, _guard: Self::Guard<'a>) {
        // SAFETY: `lock` forgot the guard it acquired, so this thread logically owns it.
        unsafe { lock_api::Mutex::force_unlock(self) };
    }
}

cfg_if::cfg_if! {
    if #[cfg(loom)] {
        pub type DefaultMutex = StdMutex;
    } else if #[cfg(feature = "parking_lot")] {
        pub type DefaultMutex = parking_lot::Mutex<()>;
    } else if #[cfg(feature = "std")] {
        pub type DefaultMutex = StdMutex;
    } else if #[cfg(all(feature = "pthread", unix))] {
        pub type DefaultMutex = PthreadMutex;
    } else {
        pub type DefaultMutex = SpinMutex;
    }
}
