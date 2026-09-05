#[cfg(all(feature = "pthread", unix))]
pub use super::pthread::PthreadCondVar;
#[cfg(feature = "std")]
pub use super::std::StdCondVar;
use crate::sync::mutex::Mutex;

// TODO every condvar related doc is generated for the moment
/// A condition variable paired with a [`Mutex`] implementation.
///
/// It is mostly a building block for [`CondVarParker`](super::parker::CondVarParker), as
/// platforms offering a native parking primitive should implement
/// [`Parker`](super::parker::Parker) directly.
///
/// # Safety
///
/// [`wait`](Self::wait) must return with `mutex` locked, and the returned guard must be a
/// valid guard for `mutex`; if the mutex is released while waiting, it must be reacquired
/// before returning.
///
/// Calls to [`notify_one`](Self::notify_one) must *synchronize-with* the
/// [`wait`](Self::wait) calls they wake up.
pub unsafe trait CondVar<M: Mutex>: Send + Sync + 'static {
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
    /// The guard must have been returned from [`Mutex::lock`] called on `mutex`, and `mutex`
    /// must be the only mutex ever paired with this condition variable.
    unsafe fn wait<'a>(&self, mutex: &'a M, guard: M::Guard<'a>) -> M::Guard<'a>;
    fn notify_one(&self);
}
