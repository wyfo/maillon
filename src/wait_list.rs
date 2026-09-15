use core::{
    marker::PhantomData,
    pin::Pin,
    sync::atomic::Ordering::{Acquire, Release, SeqCst},
    task::Waker,
};

use waker_list::WakerList;

use crate::{
    List, ListRef, Node, NodeData,
    list::{Eager, GetBack, GetFront, Linking, ListEnd, ListGetEnd, LockedList},
    loom::sync::atomic::Ordering::Relaxed,
    node::NodeRef,
    sync::mutex::{DefaultMutex, Mutex},
    wait_list::{
        synchronization::{SyncMode, Synchronization, Synchronized},
        wait::{Wait, WaitUntil, WakeCondition},
    },
};

pub mod synchronization;
pub mod wait;
mod waker_list;

pub const DEFAULT_WAKER_LIST_SIZE: usize = 32;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClosedError;

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

pub struct WaitList<
    N: Unpin = (),
    S: Synchronization = Synchronized,
    L: Linking = Eager,
    M: Mutex = DefaultMutex,
    const WAKER_LIST_SIZE: usize = DEFAULT_WAKER_LIST_SIZE,
> {
    list: List<Waiter<N>, usize, (), L, M>,
    _synchronization: PhantomData<S>,
}

impl<N: Unpin, S: Synchronization, L: Linking, M: Mutex, const WAKER_LIST_SIZE: usize> Default
    for WaitList<N, S, L, M, WAKER_LIST_SIZE>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<N: Unpin, S: Synchronization, L: Linking, M: Mutex, const WAKER_LIST_SIZE: usize>
    WaitList<N, S, L, M, WAKER_LIST_SIZE>
{
    #[cfg_attr(loom, const_fn::const_fn(cfg(false)))]
    #[inline]
    pub const fn new() -> Self {
        Self {
            list: List::with_state(STATE_OPEN),
            _synchronization: PhantomData,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        match S::MODE {
            SyncMode::Synchronized => self.list.is_empty_rmw(Release),
            SyncMode::Sequential => self.list.is_empty(SeqCst),
            SyncMode::Unsynchronized => self.list.is_empty(Relaxed),
        }
    }

    pub fn is_closed(&self) -> bool {
        self.list
            .load_state(Acquire)
            .is_some_and(|s| s != STATE_OPEN)
    }

    pub fn close(&self) {
        self.list.update_state_or_lock_with(
            Release,
            Relaxed,
            |_| STATE_CLOSED,
            |locked| Self::wake_all(locked, STATE_CLOSED, || None),
        );
    }

    #[cold]
    #[inline(never)]
    fn wake_all<F: FnMut() -> Option<Notification<N>>>(
        locked: LockedList<Waiter<N>, usize, (), L, M>,
        state: usize,
        mut notification: F,
    ) {
        let mut wakers = WakerList::<WAKER_LIST_SIZE>::new();
        locked.drain(|| state).for_each(
            &mut wakers,
            |wakers, mut waiter, _| {
                if let Some(notification) = notification() {
                    waiter.notification = Some(notification);
                }
                if let Some(waker) = waiter.waker.take() {
                    wakers.push(waker);
                }
                wakers.is_full()
            },
            |wakers| wakers.wake_all(),
        );
    }

    #[inline]
    pub fn notify_one_with<F: FnOnce() -> N>(&self, notification: F) {
        if !self.is_empty() {
            self.wake_single::<GetFront, _>(|| Notification::One(notification()));
        }
    }

    #[inline]
    pub fn notify_last_with<F: FnOnce() -> N>(&self, notification: F) {
        if !self.is_empty() {
            self.wake_single::<GetBack, _>(|| Notification::Last(notification()));
        }
    }

    #[cold]
    #[inline(never)]
    fn wake_single<E: ListGetEnd, F: FnOnce() -> Notification<N>>(&self, notification: F) {
        Self::wake_single_locked::<E, F>(self.list.lock(), notification);
    }

    fn wake_single_locked<E: ListGetEnd, F: FnOnce() -> Notification<N>>(
        mut locked: LockedList<Waiter<N>, usize, (), L, M>,
        notification: F,
    ) {
        let Some(mut waiter) = E::get_end(&mut locked) else {
            return;
        };
        waiter.data_mut().notification = Some(notification());
        let waker = waiter.data_mut().waker.take();
        waiter.unlink(|| STATE_OPEN);
        drop(locked);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    #[inline]
    pub fn notify_many_with<F: FnMut() -> N>(&self, count: usize, notification: F) {
        if !self.is_empty() {
            self.wake_many(count, notification);
        }
    }

    #[cold]
    #[inline(never)]
    fn wake_many<F: FnMut() -> N>(&self, count: usize, mut notification: F) {
        let mut wakers = WakerList::<WAKER_LIST_SIZE>::new();
        let mut locked = self.list.lock();
        let mut front = locked.front();
        for _ in 0..count {
            let Some(mut waiter) = front else {
                break;
            };
            waiter.notification = Some(Notification::One(notification()));
            if let Some(waker) = waiter.waker.take() {
                wakers.push(waker);
            }
            front = waiter.unlink(|| STATE_OPEN);
            if wakers.is_full() {
                let list = locked.unlock();
                wakers.wake_all();
                if list.is_empty(Relaxed) {
                    return;
                }
                locked = list.lock();
                front = locked.front();
            }
        }
        drop(locked);
        wakers.wake_all();
    }

    #[inline]
    pub fn notify_all_with<F: FnMut() -> N>(&self, notification: F) {
        if !self.is_empty() {
            self.notify_all_impl(notification);
        }
    }

    #[cold]
    #[inline(never)]
    fn notify_all_impl<F: FnMut() -> N>(&self, mut notification: F) {
        let locked = self.list.lock();
        Self::wake_all(locked, STATE_OPEN, || {
            Some(Notification::All(notification()))
        });
    }

    #[inline]
    pub fn wait(&self) -> Wait<'_, N, S, L, M, WAKER_LIST_SIZE> {
        Wait(Node::new(WaitListRef(self)))
    }

    #[cold]
    #[inline(never)]
    fn renotify(&self, notification: Notification<N>) {
        match notification {
            Notification::One(notification) => self.notify_one_with(|| notification),
            Notification::Last(notification) => self.notify_last_with(|| notification),
            _ => unreachable!(),
        }
    }
}

impl<S: Synchronization, L: Linking, M: Mutex, const WAKER_LIST_SIZE: usize>
    WaitList<(), S, L, M, WAKER_LIST_SIZE>
{
    #[inline]
    pub fn notify_one(&self) {
        self.notify_one_with(|| ());
    }

    #[inline]
    pub fn notify_last(&self) {
        self.notify_last_with(|| ());
    }

    #[inline]
    pub fn notify_many(&self, count: usize) {
        self.notify_many_with(count, || ());
    }

    #[inline]
    pub fn notify_all(&self) {
        self.notify_all_with(|| ());
    }

    #[inline]
    pub fn wait_until<F: FnMut(bool) -> W, W: WakeCondition>(
        &self,
        wake_condition: F,
    ) -> WaitUntil<'_, F, S, L, M, WAKER_LIST_SIZE> {
        WaitUntil::new(self.wait(), wake_condition)
    }
}

struct WaitListRef<
    'a,
    N: Unpin,
    S: Synchronization,
    L: Linking,
    M: Mutex,
    const WAKER_LIST_SIZE: usize,
>(&'a WaitList<N, S, L, M, WAKER_LIST_SIZE>);

impl<N: Unpin, S: Synchronization, L: Linking, M: Mutex, const WAKER_LIST_SIZE: usize> ListRef
    for WaitListRef<'_, N, S, L, M, WAKER_LIST_SIZE>
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

impl<'a, N: Unpin, S: Synchronization, L: Linking, M: Mutex, const WAKER_LIST_SIZE: usize>
    NodeData<WaitListRef<'a, N, S, L, M, WAKER_LIST_SIZE>> for Waiter<N>
{
    fn new_state_if_last_node_on_drop(
        self: Pin<&mut Self>,
        _list: &WaitListRef<'a, N, S, L, M, WAKER_LIST_SIZE>,
    ) -> usize {
        STATE_OPEN
    }

    fn on_drop<'list>(
        self: Pin<&mut Self>,
        list: &'list WaitListRef<'a, N, S, L, M, WAKER_LIST_SIZE>,
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
                    WaitList::<N, S, L, M, WAKER_LIST_SIZE>::wake_single_locked::<GetFront, _>(
                        locked,
                        || Notification::One(notification),
                    );
                }
                Notification::Last(notification) => {
                    WaitList::<N, S, L, M, WAKER_LIST_SIZE>::wake_single_locked::<GetBack, _>(
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
