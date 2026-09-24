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
    list::{Back, End, Front, LockedList},
    loom::sync::atomic::{Ordering::Relaxed, fence},
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

struct Waiter<N: Notification> {
    waker: Option<Waker>,
    notification: Option<Notified<N>>,
    waiter: N::Waiter,
}

impl<N: Notification> Waiter<N> {
    fn new(waiter: N::Waiter) -> Self {
        Self {
            waker: None,
            notification: None,
            waiter,
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

enum Notified<N> {
    One(N),
    Last(N),
    All(N),
}

impl<N> Notified<N> {
    fn into_inner(self) -> N {
        match self {
            Self::One(notification) | Self::Last(notification) | Self::All(notification) => {
                notification
            }
        }
    }
}

/// Notification sent by [`WaitList`]'s `notify_*` methods, matched against waiter data.
pub trait Notification: Unpin {
    /// Data registered with the waiter.
    type Waiter: Unpin;

    /// Returns `true` if the notification can be sent to the waiter.
    fn matches(&self, waiter: &Self::Waiter) -> bool;
}

impl Notification for () {
    type Waiter = ();

    fn matches(&self, _waiter: &Self::Waiter) -> bool {
        true
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
/// Notifications can be used to pass data to waiters, and to filter the ones which will be
/// notified (except with `notify_all` which doesn't filter).
///
/// # Synchronization
///
/// `WaitList` has a generic `S` parameter which determines the synchronization guarantees. See
/// [`Synchronization`] documentation for more details about its variants.
///
/// With the default [`Synchronized`], the wake condition can be accessed with `Relaxed` ordering.
///
/// # Linking and locking
///
/// `WaitList` is built on [`List`] and inherits its [`Linking`] and [`Mutex`] parameters.
///
/// The mutex is mostly used to wake waiters, but calling `notify_*` will not acquire the mutex if
/// there is no registered waiter.
///
/// # Waker batching
///
/// Waiters' wakers are always woken after releasing the list lock. With `notify_all`/`notify_many`,
/// wakers are first accumulated in batches of `WAKER_BATCH_SIZE`, and the lock is temporarily
/// released to wake them.
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
pub struct WaitList<
    N: Notification = (),
    S: Synchronization = Synchronized,
    L: Linking = AtomicEager,
    M: Mutex = DefaultMutex,
    const WAKER_BATCH_SIZE: usize = DEFAULT_WAKER_BATCH_SIZE,
> {
    list: List<Waiter<N>, usize, (), L, M>,
    _synchronization: PhantomData<S>,
}

impl<N: Notification, S: Synchronization, L: Linking, M: Mutex, const WAKER_BATCH_SIZE: usize>
    Default for WaitList<N, S, L, M, WAKER_BATCH_SIZE>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<N: Notification, S: Synchronization, L: Linking, M: Mutex, const WAKER_BATCH_SIZE: usize>
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
        if S::SYNC {
            fence(SeqCst);
        }
        self.list.is_empty(match S::MODE {
            SyncMode::Sequential => SeqCst,
            _ => Relaxed,
        })
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
    fn wake_all<F: FnMut() -> Option<Notified<N>>>(
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

    /// Notifies the first registered waiter matching the notification.
    ///
    /// If the notified waiter is dropped before consuming the notification, it is passed to the
    /// first matching waiter registered at that time.
    ///
    /// Returns `true` if a waiter has been notified.
    #[inline]
    pub fn notify_one_with<F: FnOnce() -> N>(&self, notification: F) -> bool {
        !self.is_empty() && self.wake_single::<Front, _>(notification)
    }

    /// Notifies the last registered waiter matching the notification.
    ///
    /// If the notified waiter is dropped before consuming the notification, it is passed to the
    /// last matching waiter registered at that time.
    ///
    /// Returns `true` if a waiter has been notified.
    #[inline]
    pub fn notify_last_with<F: FnOnce() -> N>(&self, notification: F) -> bool {
        !self.is_empty() && self.wake_single::<Back, _>(notification)
    }

    #[cold]
    fn wake_single<E: End, F: FnOnce() -> N>(&self, notification: F) -> bool {
        Self::wake_single_locked::<E, F>(self.list.lock(), notification)
    }

    fn wake_single_locked<E: End, F: FnOnce() -> N>(
        mut locked: LockedList<Waiter<N>, usize, (), L, M>,
        notification: F,
    ) -> bool {
        let Some(mut waiter) = locked.end::<E>() else {
            return false;
        };
        let notification = notification();
        let notified = if E::IS_FRONT {
            Notified::One
        } else {
            Notified::Last
        };
        let waker = if notification.matches(&waiter.data().waiter) {
            waiter.data_mut().notification = Some(notified(notification));
            let waker = waiter.data_mut().waker.take();
            waiter.unlink(|_, _| STATE_OPEN);
            waker
        } else {
            let mut cursor = waiter.into_cursor();
            loop {
                if E::IS_FRONT {
                    cursor.move_next();
                } else {
                    cursor.move_prev();
                }
                let Some(mut waiter) = cursor.current() else {
                    return false;
                };
                if notification.matches(&waiter.waiter) {
                    waiter.notification = Some(notified(notification));
                    let waker = waiter.waker.take();
                    cursor.remove_current(|_, _| STATE_OPEN);
                    break waker;
                }
            }
        };
        drop(locked);
        if let Some(waker) = waker {
            waker.wake();
        }
        true
    }

    /// Notifies the first `count` registered waiters matching the notification.
    ///
    /// If a notified waiter is dropped before consuming the notification, it is passed to the
    /// first matching waiter registered at that time.
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
    fn wake_many<F: FnMut() -> N>(&self, count: usize, mut notification: F) -> usize {
        let mut wakers = WakerBatch::<WAKER_BATCH_SIZE>::new();
        let mut locked = self.list.lock();
        let mut cursor = locked.cursor_front();
        let mut notified = 0;
        while notified < count {
            let Some(mut waiter) = cursor.current() else {
                break;
            };
            let notification = notification();
            if !notification.matches(&waiter.waiter) {
                cursor.move_next();
                continue;
            }
            waiter.notification = Some(Notified::One(notification));
            if let Some(waker) = waiter.waker.take() {
                wakers.push(waker);
            }
            cursor.remove_current(|_, _| STATE_OPEN);
            notified += 1;
            if wakers.is_full() {
                let list = locked.unlock();
                wakers.wake_all();
                if list.is_empty(Relaxed) {
                    return notified;
                }
                locked = list.lock();
                cursor = locked.cursor_front();
            }
        }
        drop(locked);
        wakers.wake_all();
        notified
    }

    /// Notifies all the registered waiters, whether they match the notification or not.
    ///
    /// If a notified waiter is dropped before consuming the notification, it is lost.
    ///
    /// The operation is atomic: if a new waiter is registered while `notify_all` is ongoing, it
    /// will not be notified.
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
    fn notify_all_impl<F: FnMut() -> N>(&self, mut notification: F) -> usize {
        let locked = self.list.lock();
        Self::wake_all(locked, STATE_OPEN, || Some(Notified::All(notification())))
    }

    /// Waits for a notification matching the given waiter data.
    ///
    /// The returned future registers the task waker in the wait list at its first poll, and
    /// completes with the notification once notified. If the wait list is closed, or gets closed
    /// while waiting, it completes with [`ClosedError`].
    ///
    /// Dropping the future unregisters its waker. If it has been notified by
    /// `notify_one`/`notify_last`/`notify_many` but not polled to completion, the notification is
    /// passed on to another waiter.
    #[inline]
    pub fn wait_with(&self, waiter: N::Waiter) -> Wait<'_, N, S, L, M, WAKER_BATCH_SIZE> {
        Wait(Node::with_data(WaitListRef(self), Waiter::new(waiter)))
    }

    /// Waits until the given wake condition is satisfied, or until a satisfying notification is
    /// received.
    ///
    /// At each poll of the returned future, the wake condition is checked before and after
    /// registering the task waker. The closure is passed a boolean telling whether the waker is
    /// already registered when it is called; it can be used to relax the first check when a
    /// non-default [`Synchronization`] is used.
    ///
    /// The future completes with the wake condition output as soon as it is satisfied, and with
    /// [`ClosedError`] if the wait list is closed while the condition is not satisfied.
    ///
    /// Notifier threads should call `notify_*` after the wake condition is satisfied.
    #[inline]
    pub fn wait_until_with<F: FnMut(bool) -> W, G: FnMut(N) -> W, W: WakeCondition>(
        &self,
        waiter: N::Waiter,
        wake_condition: F,
        on_notification: G,
    ) -> WaitUntil<'_, F, G, N, S, L, M, WAKER_BATCH_SIZE> {
        WaitUntil::new(self.wait_with(waiter), wake_condition, on_notification)
    }

    #[cold]
    fn renotify(&self, notification: Notified<N>) {
        match notification {
            Notified::One(notification) => self.notify_one_with(|| notification),
            Notified::Last(notification) => self.notify_last_with(|| notification),
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

    /// Notifies the first `count` registered waiters.
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

    /// Waits for a notification.
    ///
    /// See [`wait_with`](Self::wait_with).
    #[inline]
    pub fn wait(&self) -> Wait<'_, (), S, L, M, WAKER_BATCH_SIZE> {
        self.wait_with(())
    }

    /// Waits until the given wake condition is satisfied.
    ///
    /// See [`wait_until_with`](Self::wait_until_with).
    #[inline]
    #[allow(clippy::type_complexity)]
    pub fn wait_until<F: FnMut(bool) -> W, W: WakeCondition>(
        &self,
        wake_condition: F,
    ) -> WaitUntil<'_, F, fn(()) -> W, (), S, L, M, WAKER_BATCH_SIZE> {
        self.wait_until_with((), wake_condition, |_| W::default())
    }
}

struct WaitListRef<
    'a,
    N: Notification,
    S: Synchronization,
    L: Linking,
    M: Mutex,
    const WAKER_BATCH_SIZE: usize,
>(&'a WaitList<N, S, L, M, WAKER_BATCH_SIZE>);

impl<N: Notification, S: Synchronization, L: Linking, M: Mutex, const WAKER_BATCH_SIZE: usize>
    ListRef for WaitListRef<'_, N, S, L, M, WAKER_BATCH_SIZE>
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

impl<'a, N: Notification, S: Synchronization, L: Linking, M: Mutex, const WAKER_BATCH_SIZE: usize>
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
        let Some(notif @ (Notified::One(_) | Notified::Last(_))) =
            self.get_mut().notification.take()
        else {
            return;
        };
        if let Some(locked) = locked {
            debug_assert!(!state_updated_on_unlink);
            match notif {
                Notified::One(notification) => {
                    WaitList::<N, S, L, M, WAKER_BATCH_SIZE>::wake_single_locked::<Front, _>(
                        locked,
                        || notification,
                    );
                }
                Notified::Last(notification) => {
                    WaitList::<N, S, L, M, WAKER_BATCH_SIZE>::wake_single_locked::<Back, _>(
                        locked,
                        || notification,
                    );
                }
                _ => unreachable!(),
            }
        } else {
            list.0.renotify(notif);
        }
    }
}
