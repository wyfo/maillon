//! An asynchronous wait list with customizable synchronization, built on top of [`List`].
use core::{
    fmt,
    marker::PhantomData,
    pin::Pin,
    sync::atomic::Ordering::{Acquire, Release, SeqCst},
    task::Waker,
};

#[allow(unused_imports)]
use crate::msrv::OptionExt;
use crate::{
    List, ListRef, Node, NodeData,
    linking::{AtomicEager, Linking},
    list::{GetBack, GetFront, ListEnd, ListGetEnd, LockedList},
    loom::sync::atomic::Ordering::Relaxed,
    node::NodeRef,
    sync::mutex::{DefaultMutex, Mutex},
    wait_list::{
        synchronization::{SyncMode, Synchronization, Synchronized},
        wait::{Wait, WaitUntil, WakeCondition},
    },
    waker_batch::WakerBatch,
};

pub mod synchronization;
pub mod wait;

/// Default `WAKER_BATCH_SIZE` of [`WaitList`].
pub const DEFAULT_WAKER_BATCH_SIZE: usize = 32;

const STATE_OPEN: usize = 0;
const STATE_CLOSED: usize = 1;

struct Waiter<N> {
    waker: Option<Waker>,
    notification: Option<Notification<N>>,
}

impl<N> Default for Waiter<N> {
    fn default() -> Self {
        Self {
            waker: None,
            notification: None,
        }
    }
}

/// Error returned by [`Wait`] and [`WaitUntil`] futures when the wait list is closed.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClosedError;

impl fmt::Display for ClosedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("wait list is closed")
    }
}

#[cfg(feature = "std")]
impl std::error::Error for ClosedError {}

enum Notification<N> {
    One(N),
    Last(N),
    All(N),
}

impl<N> Notification<N> {
    fn into_inner(self) -> N {
        match self {
            Self::One(notification) | Self::Last(notification) | Self::All(notification) => {
                notification
            }
        }
    }
}

/// An asynchronous wait list.
///
/// Tasks register through [`wait`] or [`wait_until`], and are woken by `notify_*` methods.
///
/// `WaitList` should be paired with a wake condition, satisfied **before** notifying the tasks, and
/// checked **after** registering the tasks, i.e. polling the `wait`/`wait_until` futures, to not
/// miss a concurrent notification that happened before.
///
/// `WaitList` can be closed, in which case all waiters complete with [`ClosedError`].
///
/// # Notification
///
/// Each woken waiter receives a notification of type `N` (`()` by default). Notifications sent by
/// [`notify_one`]/[`notify_last`]/[`notify_many`] are passed on to another waiter if the notified
/// one is dropped, i.e. canceled, before consuming it, while [`notify_all`] notifications are lost.
///
/// # Synchronization
///
/// `WaitList` has a generic `S` parameter which determines the synchronization guarantees. See
/// [`Synchronization`] documentation for more details about its variants.
///
/// With the default [`Synchronized`], polling `wait`/`wait_until` futures "acquires" all memory
/// "released" by calls to `notify_*` before the polling. Later calls to `notify_*` will wake the
/// registered tasks.
///
/// # Linking and locking
///
/// `WaitList` is built on [`List`] and inherits its [`Linking`] and [`Mutex`] parameters.
///
/// The mutex is used to wake waiters (and to register them with [`Serialized`] linking), but
/// calling `notify_*` will not acquire the mutex if there is no registered waiter.
///
/// # Waker batching
///
/// Waiters' wakers are always woken after releasing the list lock. With `notify_all`/`notify_many`,
/// wakers are first accumulated in batches of `WAKER_BATCH_SIZE`, and the lock is temporarily
/// released to wake them.
///
/// `notify_all` is atomic: if a new waiter is registered while `notify_all` is ongoing, it will
/// not be notified.
///
/// # Examples
///
/// ```
/// use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
///
/// use maillon::WaitList;
///
/// #[derive(Default)]
/// pub struct Event {
///     done: AtomicBool,
///     wait_list: WaitList,
/// }
///
/// impl Event {
///     pub async fn wait(&self) {
///         let _ = self.wait_list.wait_until(|_| self.done.load(Relaxed)).await;
///     }
///
///     pub fn set(&self) {
///         self.done.store(true, Relaxed);
///         self.wait_list.notify_all();
///     }
/// }
/// ```
///
/// [`wait`]: Self::wait
/// [`wait_until`]: Self::wait_until
/// [`notify_one`]: Self::notify_one
/// [`notify_last`]: Self::notify_last
/// [`notify_many`]: Self::notify_many
/// [`notify_all`]: Self::notify_all
/// [`Serialized`]: crate::linking::Serialized
pub struct WaitList<
    N: Unpin = (),
    S: Synchronization = Synchronized,
    L: Linking = AtomicEager,
    M: Mutex = DefaultMutex,
    const WAKER_BATCH_SIZE: usize = DEFAULT_WAKER_BATCH_SIZE,
> {
    list: List<Waiter<N>, usize, (), L, M>,
    _synchronization: PhantomData<S>,
}

impl<N: Unpin, S: Synchronization, L: Linking, M: Mutex, const WAKER_BATCH_SIZE: usize> Default
    for WaitList<N, S, L, M, WAKER_BATCH_SIZE>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<N: Unpin, S: Synchronization, L: Linking, M: Mutex, const WAKER_BATCH_SIZE: usize>
    WaitList<N, S, L, M, WAKER_BATCH_SIZE>
{
    /// Creates an empty wait list.
    #[cfg_attr(loom, const_fn::const_fn(cfg(false)))]
    #[inline]
    pub const fn new() -> Self {
        Self {
            list: List::with_state(STATE_OPEN),
            _synchronization: PhantomData,
        }
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        match S::MODE {
            SyncMode::Synchronized => self.list.is_empty_rmw(Release),
            SyncMode::Sequential => self.list.is_empty(SeqCst),
            SyncMode::Unsynchronized => self.list.is_empty(Relaxed),
        }
    }

    /// Returns `true` if the wait list is closed.
    #[allow(clippy::incompatible_msrv)]
    pub fn is_closed(&self) -> bool {
        self.list
            .load_state(Acquire)
            .is_some_and(|s| s != STATE_OPEN)
    }

    /// Closes the wait list, waking all the waiters.
    ///
    /// Current and future [`wait`](Self::wait)/[`wait_until`](Self::wait_until) futures complete
    /// with [`ClosedError`], unless `wait_until`'s wake condition is already satisfied.
    pub fn close(&self) {
        self.list.update_state_or_lock_with(
            Release,
            Relaxed,
            |_| STATE_CLOSED,
            |locked| {
                Self::wake_all(locked, STATE_CLOSED, || None);
            },
        );
    }

    #[cold]
    #[inline(never)]
    fn wake_all<F: FnMut() -> Option<Notification<N>>>(
        locked: LockedList<Waiter<N>, usize, (), L, M>,
        state: usize,
        mut notification: F,
    ) -> usize {
        locked
            .drain(|_| state)
            .wake_all::<WAKER_BATCH_SIZE, _>(|mut waiter, _| {
                if let Some(notification) = notification() {
                    waiter.notification = Some(notification);
                }
                waiter.waker.take()
            })
    }

    /// Notifies the first registered waiter with `notification()`.
    ///
    /// `notification` is only called if there is a waiter. If the waiter is dropped before
    /// consuming the notification, it is passed on to the first waiter registered at that time.
    ///
    /// Returns `true` if a waiter has been notified.
    #[inline]
    pub fn notify_one_with<F: FnOnce() -> N>(&self, notification: F) -> bool {
        !self.is_empty() && self.wake_single::<GetFront, _>(|| Notification::One(notification()))
    }

    /// Notifies the last registered waiter with `notification()`.
    ///
    /// `notification` is only called if there is a waiter. If the waiter is dropped before
    /// consuming the notification, it is passed on to the last waiter registered at that time,
    /// which may have been registered after the dropped one.
    ///
    /// Returns `true` if a waiter has been notified.
    #[inline]
    pub fn notify_last_with<F: FnOnce() -> N>(&self, notification: F) -> bool {
        !self.is_empty() && self.wake_single::<GetBack, _>(|| Notification::Last(notification()))
    }

    #[cold]
    #[inline(never)]
    fn wake_single<E: ListGetEnd, F: FnOnce() -> Notification<N>>(&self, notification: F) -> bool {
        Self::wake_single_locked::<E, F>(self.list.lock(), notification)
    }

    fn wake_single_locked<E: ListGetEnd, F: FnOnce() -> Notification<N>>(
        mut locked: LockedList<Waiter<N>, usize, (), L, M>,
        notification: F,
    ) -> bool {
        let Some(mut waiter) = E::get_end(&mut locked) else {
            return false;
        };
        waiter.data_mut().notification = Some(notification());
        let waker = waiter.data_mut().waker.take();
        waiter.unlink(|_, _| STATE_OPEN);
        drop(locked);
        if let Some(waker) = waker {
            waker.wake();
        }
        true
    }

    /// Notifies up to `count` waiters, in registration order, each one with `notification()`.
    ///
    /// `notification` is called once per notified waiter. If a waiter is dropped before
    /// consuming its notification, it is passed on to the first waiter registered at that time.
    ///
    /// Returns the number of notified waiters.
    #[inline]
    pub fn notify_many_with<F: FnMut() -> N>(&self, count: usize, notification: F) -> usize {
        if self.is_empty() {
            return 0;
        }
        self.wake_many(count, notification)
    }

    #[cold]
    #[inline(never)]
    fn wake_many<F: FnMut() -> N>(&self, count: usize, mut notification: F) -> usize {
        let mut wakers = WakerBatch::<WAKER_BATCH_SIZE>::new();
        let mut locked = self.list.lock();
        let mut front = locked.front();
        let mut notified = 0;
        while notified < count {
            let Some(mut waiter) = front else {
                break;
            };
            waiter.notification = Some(Notification::One(notification()));
            if let Some(waker) = waiter.waker.take() {
                wakers.push(waker);
            }
            front = waiter.unlink(|_, _| STATE_OPEN);
            notified += 1;
            if wakers.is_full() {
                let list = locked.unlock();
                wakers.wake_all();
                if list.is_empty(Relaxed) {
                    return notified;
                }
                locked = list.lock();
                front = locked.front();
            }
        }
        drop(locked);
        wakers.wake_all();
        notified
    }

    /// Notifies all the registered waiters, each one with `notification()`.
    ///
    /// `notification` is called once per waiter. Contrary to the other `notify_*` methods, the
    /// notification is lost if the waiter is dropped before consuming it.
    ///
    /// Returns the number of notified waiters.
    #[inline]
    pub fn notify_all_with<F: FnMut() -> N>(&self, notification: F) -> usize {
        if self.is_empty() {
            return 0;
        }
        self.notify_all_impl(notification)
    }

    #[cold]
    #[inline(never)]
    fn notify_all_impl<F: FnMut() -> N>(&self, mut notification: F) -> usize {
        let locked = self.list.lock();
        Self::wake_all(locked, STATE_OPEN, || {
            Some(Notification::All(notification()))
        })
    }

    /// Waits for a notification.
    ///
    /// The returned future registers the task waker in the wait list at its first poll, and
    /// completes with the notification once notified. If the wait list is closed, or gets closed
    /// while waiting, it completes with [`ClosedError`].
    ///
    /// Dropping the future unregisters its waker. If it has been notified by
    /// `notify_one`/`notify_last`/`notify_many` but not polled to completion, the notification is
    /// passed on to another waiter.
    #[inline]
    pub fn wait(&self) -> Wait<'_, N, S, L, M, WAKER_BATCH_SIZE> {
        Wait(Node::new(WaitListRef(self)))
    }

    #[cold]
    #[inline(never)]
    fn renotify(&self, notification: Notification<N>) {
        match notification {
            Notification::One(notification) => self.notify_one_with(|| notification),
            Notification::Last(notification) => self.notify_last_with(|| notification),
            _ => unreachable!(),
        };
    }
}

impl<S: Synchronization, L: Linking, M: Mutex, const WAKER_BATCH_SIZE: usize>
    WaitList<(), S, L, M, WAKER_BATCH_SIZE>
{
    /// Notifies the first registered waiter.
    ///
    /// See [`notify_one_with`](Self::notify_one_with).
    #[inline]
    pub fn notify_one(&self) -> bool {
        self.notify_one_with(|| ())
    }

    /// Notifies the last registered waiter.
    ///
    /// See [`notify_last_with`](Self::notify_last_with).
    #[inline]
    pub fn notify_last(&self) -> bool {
        self.notify_last_with(|| ())
    }

    /// Notifies up to `count` waiters, in registration order.
    ///
    /// See [`notify_many_with`](Self::notify_many_with).
    #[inline]
    pub fn notify_many(&self, count: usize) -> usize {
        self.notify_many_with(count, || ())
    }

    /// Notifies all the registered waiters.
    ///
    /// See [`notify_all_with`](Self::notify_all_with).
    #[inline]
    pub fn notify_all(&self) -> usize {
        self.notify_all_with(|| ())
    }

    /// Waits until the given wake condition is satisfied.
    ///
    /// At each poll of the returned future, the wake condition is checked; if it is not
    /// satisfied, the task waker is registered in the wait list, and the condition is checked
    /// again, so that no notification can be missed. The closure is passed a boolean telling
    /// whether the waker is already registered when it is called; it can be used to relax the
    /// first check when a non-default [`Synchronization`] is used.
    ///
    /// Notifier threads should call `notify_*` after the wake condition is satisfied.
    ///
    /// The future completes with the wake condition output as soon as it is satisfied, and with
    /// [`ClosedError`] if the wait list is closed while the condition is not satisfied.
    /// Notifications alone do not complete it: a woken future checks the condition again, and
    /// registers its waker again if it is still unsatisfied.
    #[inline]
    pub fn wait_until<F: FnMut(bool) -> W, W: WakeCondition>(
        &self,
        wake_condition: F,
    ) -> WaitUntil<'_, F, S, L, M, WAKER_BATCH_SIZE> {
        WaitUntil::new(self.wait(), wake_condition)
    }
}

struct WaitListRef<
    'a,
    N: Unpin,
    S: Synchronization,
    L: Linking,
    M: Mutex,
    const WAKER_BATCH_SIZE: usize,
>(&'a WaitList<N, S, L, M, WAKER_BATCH_SIZE>);

impl<N: Unpin, S: Synchronization, L: Linking, M: Mutex, const WAKER_BATCH_SIZE: usize> ListRef
    for WaitListRef<'_, N, S, L, M, WAKER_BATCH_SIZE>
{
    type NodeData = Waiter<N>;
    type ListState = usize;
    type ListData = ();
    type Linking = L;
    type Mutex = M;

    fn as_list(&self) -> &List<Waiter<N>, usize, (), L, M> {
        &self.0.list
    }
}

impl<'a, N: Unpin, S: Synchronization, L: Linking, M: Mutex, const WAKER_BATCH_SIZE: usize>
    NodeData<WaitListRef<'a, N, S, L, M, WAKER_BATCH_SIZE>> for Waiter<N>
{
    fn new_state_if_last_node_on_drop(
        self: Pin<&mut Self>,
        _list: &WaitListRef<'a, N, S, L, M, WAKER_BATCH_SIZE>,
        _list_data: &mut (),
    ) -> usize {
        STATE_OPEN
    }

    fn on_drop<'list>(
        self: Pin<&mut Self>,
        list: &'list WaitListRef<'a, N, S, L, M, WAKER_BATCH_SIZE>,
        locked: Option<LockedList<'list, Self, usize, (), L, M>>,
        state_updated_on_unlink: bool,
    ) {
        let Some(notif @ (Notification::One(_) | Notification::Last(_))) =
            self.get_mut().notification.take()
        else {
            return;
        };
        if let Some(locked) = locked {
            debug_assert!(!state_updated_on_unlink);
            match notif {
                Notification::One(notification) => {
                    WaitList::<N, S, L, M, WAKER_BATCH_SIZE>::wake_single_locked::<GetFront, _>(
                        locked,
                        || Notification::One(notification),
                    );
                }
                Notification::Last(notification) => {
                    WaitList::<N, S, L, M, WAKER_BATCH_SIZE>::wake_single_locked::<GetBack, _>(
                        locked,
                        || Notification::Last(notification),
                    );
                }
                _ => unreachable!(),
            }
        } else {
            list.0.renotify(notif);
        }
    }
}
