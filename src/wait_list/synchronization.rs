//! [`Synchronization`] and its variants.
use core::fmt::Debug;

/// Generic parameter of [`WaitList`] which determines its synchronization guarantees.
///
/// As a consequence, it impacts how the wake condition should be accessed.
///
/// `WaitList` uses the `store X; load Y || store Y; load X` pattern, where `X` is the wake
/// condition, and `Y` the waker registration state (`load Y` is done in `notify_*` while
/// `store Y` corresponds to [`wait`]). There are three main ways to make this pattern work, i.e.,
/// either `load Y` sees a waker registered, or `load X` sees the wake condition satisfied:
/// - every operation uses `SeqCst`
/// - insert `SeqCst` fences between stores and loads
/// - use RMW operations for `X` store + load, with `Acquire` ordering for store and `Release`
///   ordering for load
///
/// (There is also a symmetric way to the last one, using RMW operations for `Y` store + load, but
/// `SeqCst` fences are almost always better in practice, especially because `Y` becomes read-only
/// in the usual `store X; load Y` hot path).
///
/// This gives the following variants:
/// - [`Synchronized`] (the default), inserting `SeqCst` fences between `WaitList` (`Y`)
///   operations and wake condition (`X`) operations, which can be `Relaxed`
/// - [`Sequential`], using `SeqCst` operations in `WaitList` (`Y`) and relying on `SeqCst`
///   operations on the wake condition (`X`) to be used
/// - [`Unsynchronized`], relying on external `SeqCst` fences or RMW operations with appropriate
///   ordering on the wake condition (`X`) to be used
///
/// While `Sequential` and `Unsynchronized` put requirements on the wake condition check, they only
/// concern the check after the registration. Checks executed before, as done by [`wait_until`],
/// can be relaxed. For example, with `Unsynchronized`, a first check can omit a `SeqCst` fence, or
/// replace an RMW by a load.
///
/// # Which variant to choose
///
/// In doubt, use the default one which will work in all cases. Otherwise, the choice depends
/// mainly on the existing constraints on the wake condition accesses, on the architecture, and on
/// the operation to optimize.
///
/// For example, if the wake condition is already accessed through RMW, and the appropriate
/// orderings are cheap to add (RMW ordering makes no difference on x86), `Sequential` or
/// `Unsynchronized` should be considered.
///
/// In any case, profiling and benchmarking the different variants will often give the best answer.
///
/// [`WaitList`]: crate::wait_list::WaitList
/// [`wait`]: crate::wait_list::WaitList::wait
/// [`wait_until`]: crate::wait_list::WaitList::wait_until
pub trait Synchronization:
    private::PrivateSynchronization + Send + Sync + Debug + Sized + 'static
{
}

mod private {
    pub trait PrivateSynchronization {
        const MODE: SyncMode;
        const SYNC: bool = matches!(Self::MODE, SyncMode::Synchronized);
    }

    pub enum SyncMode {
        Synchronized,
        Sequential,
        Unsynchronized,
    }
}
pub(crate) use private::SyncMode;

/// [`WaitList`] inserts `SeqCst` fences before `notify_*` and after the waker registration in
/// [`Wait`]/[`WaitUntil`] polling.
///
/// This is the default and the simplest mode; it has no requirement on the wake condition access,
/// which can use `Relaxed` ordering.
///
/// [`WaitList`]: crate::wait_list::WaitList
/// [`Wait`]: crate::wait_list::wait::Wait
/// [`WaitUntil`]: crate::wait_list::wait::WaitUntil
#[derive(Debug)]
pub struct Synchronized;
impl Synchronization for Synchronized {}
impl private::PrivateSynchronization for Synchronized {
    const MODE: SyncMode = SyncMode::Synchronized;
}

/// [`WaitList`] uses `SeqCst` ordering internally.
///
/// It requires the wake condition to be accessed using `SeqCst` ordering.
///
/// As a consequence, when there is no waker registered, `notify_*` becomes a simple `SeqCst`
/// load, thus a read-only operation with minimal contention on `WaitList` cache-line.
///
/// [`WaitList`]: crate::wait_list::WaitList
#[derive(Debug)]
pub struct Sequential;
impl Synchronization for Sequential {}
impl private::PrivateSynchronization for Sequential {
    const MODE: SyncMode = SyncMode::Sequential;
}

/// [`WaitList`] relies on external synchronization between `notify_*` and [`wait`].
///
/// As described in [`Synchronization`] documentation, it requires either:
/// - `SeqCst` fences to be inserted before `notify_*` and after the waker registration
/// - the wake condition to be stored with an `Acquire` RMW operation and to be loaded
///   with a `Release` RMW operation.
///
/// As a consequence, when there is no waker registered, `notify_*` becomes a simple `Relaxed`
/// load, thus a read-only operation with minimal contention on `WaitList` cache-line.
///
/// [`WaitList`]: crate::wait_list::WaitList
/// [`wait`]: crate::wait_list::WaitList::wait
#[derive(Debug)]
pub struct Unsynchronized;
impl Synchronization for Unsynchronized {}
impl private::PrivateSynchronization for Unsynchronized {
    const MODE: SyncMode = SyncMode::Unsynchronized;
}
