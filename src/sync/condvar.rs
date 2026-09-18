//! The [`CondVar`] abstraction and its implementations.
#[cfg(all(feature = "pthread", unix))]
pub use super::pthread::PthreadCondVar;
use crate::sync::mutex::Mutex;

/// A condition variable abstraction generic over its [`Mutex`].
///
/// It is mostly a building block for [`CondVarParker`](super::parker::CondVarParker).
///
/// # Safety
///
/// [`wait`](Self::wait) must return with `mutex` locked by the current thread, and the returned
/// guard must be a valid guard for it.
///
/// Calls to [`notify_one`](Self::notify_one) must *synchronize-with* the
/// [`wait`](Self::wait) calls they wake up.
pub unsafe trait CondVar<M: Mutex>: Send + Sync + 'static {
    /// Initial value for a condition variable.
    const INIT: Self;
    #[doc(hidden)]
    fn new() -> Self
    where
        Self: Sized,
    {
        Self::INIT
    }
    /// Blocks the current thread until this condition variable receives a notification.
    ///
    /// # Safety
    ///
    /// The guard must have been returned from [`Mutex::lock`] called on `mutex`, and `mutex`
    /// must be the only mutex ever paired with this condition variable.
    unsafe fn wait<'a>(&self, mutex: &'a M, guard: M::Guard<'a>) -> M::Guard<'a>;
    /// Wakes up one blocked thread on this condvar.
    fn notify_one(&self);
}
