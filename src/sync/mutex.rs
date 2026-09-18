//! The [`Mutex`] abstraction and its implementations.
#[cfg(all(feature = "pthread", unix))]
pub use super::pthread::PthreadMutex;
pub use super::spin::SpinMutex;

/// A raw mutex abstraction.
///
/// *This trait only exists because `std::sync::Mutex` [can't implement] `lock_api::RawMutex`.*
///
/// # Safety
///
/// Implementations of this trait must ensure that the mutex is actually
/// exclusive: a lock can't be acquired while the mutex is already locked.
///
/// Calls to [`unlock`](Self::unlock) must *synchronize-with* subsequent calls to
/// [`lock`](Self::lock).
///
/// [can't implement]: https://internals.rust-lang.org/t/unsafe-low-level-mutex/24565
pub unsafe trait Mutex: Send + Sync + 'static {
    /// Initial value for an unlocked mutex.
    const INIT: Self;

    #[doc(hidden)]
    fn new() -> Self
    where
        Self: Sized,
    {
        Self::INIT
    }

    /// An optional guard carrying the locked state of the mutex.
    type Guard<'a>
    where
        Self: 'a;

    /// Acquires this mutex, blocking the current thread until it is able to do so.
    fn lock(&self) -> Self::Guard<'_>;

    /// Unlocks this mutex.
    ///
    /// # Safety
    ///
    /// The guard must have been returned from [`lock`](Self::lock) called on this very mutex, and
    /// must be used only once, on the same thread the guard was acquired on.
    unsafe fn unlock<'a>(&'a self, guard: Self::Guard<'a>);
}

#[cfg(feature = "lock_api")]
unsafe impl<R: lock_api::RawMutex + Send + Sync + 'static> Mutex for lock_api::Mutex<R, ()> {
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
        type DefaultMutexImpl = crate::loom::sync::Mutex<()>;
    } else if #[cfg(feature = "parking_lot")] {
        type DefaultMutexImpl = parking_lot::Mutex<()>;
    } else if #[cfg(feature = "std")] {
        type DefaultMutexImpl = crate::loom::sync::Mutex<()>;
    } else if #[cfg(all(feature = "pthread", unix))] {
        type DefaultMutexImpl = PthreadMutex;
    } else {
        type DefaultMutexImpl = SpinMutex;
    }
}

/// The default mutex implementation used by [`List`](crate::List).
///
/// It is selected from the enabled features, by decreasing priority: `parking_lot`
/// (`parking_lot::Mutex<()>`), `std` (`std::sync::Mutex<()>`), `pthread` (`PthreadMutex`,
/// unix only), and [`SpinMutex`] otherwise.
pub type DefaultMutex = DefaultMutexImpl;
