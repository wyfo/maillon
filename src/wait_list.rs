use core::{
    hint::assert_unchecked,
    marker::PhantomData,
    mem::MaybeUninit,
    pin::Pin,
    sync::atomic::Ordering::{Acquire, Release, SeqCst},
    task::Waker,
};

use crate::{
    List, Node, as_list,
    list::{Eager, GetBack, GetFront, Linking, ListEnd, ListGetEnd, LockedList},
    loom::sync::atomic::Ordering::Relaxed,
    node::{NodeData, NodeRef},
    sync::mutex::{DefaultMutex, Mutex},
    wait_list::{
        synchronization::{SyncMode, Synchronization, Synchronized},
        wait::{Wait, WaitUntil, WakeCondition},
    },
};

pub mod synchronization;
pub mod wait;

const STATE_OPEN: usize = 0;
const STATE_CLOSED: usize = 1;

#[derive(Default)]
struct Waiter {
    waker: Option<Waker>,
    notification: Option<Notification>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClosedError;

#[derive(Clone, Copy)]
enum Notification {
    One,
    Last,
    All,
}

pub struct WaitList<S: Synchronization = Synchronized, L: Linking = Eager, M: Mutex = DefaultMutex>
{
    list: List<Waiter, usize, L, M>,
    _synchronization: PhantomData<S>,
}

impl<S: Synchronization, L: Linking, M: Mutex> Default for WaitList<S, L, M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Synchronization, L: Linking, M: Mutex> WaitList<S, L, M> {
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
            |locked| Self::wake_all(locked, STATE_CLOSED, None),
        );
    }

    #[cold]
    #[inline(never)]
    fn wake_all(
        locked: LockedList<Waiter, usize, L, M>,
        state: usize,
        notification: Option<Notification>,
    ) {
        let mut wakers = WakerList::new();
        locked.drain(|| state).for_each(
            &mut wakers,
            |wakers, mut waiter| {
                if let Some(notification) = notification {
                    waiter.notification = Some(notification);
                }
                wakers.push(unsafe { waiter.waker.take().unwrap_unchecked() });
                wakers.is_full()
            },
            |wakers| wakers.drain().for_each(Waker::wake),
        );
    }

    #[inline]
    pub fn notify_one(&self) {
        if !self.is_empty() {
            self.wake_single::<GetFront>(Notification::One);
        }
    }

    #[inline]
    pub fn notify_last(&self) {
        if !self.is_empty() {
            self.wake_single::<GetBack>(Notification::Last);
        }
    }

    #[cold]
    #[inline(never)]
    fn wake_single<E: ListGetEnd>(&self, notification: Notification) {
        Self::wake_single_locked::<E>(self.list.lock(), notification);
    }

    fn wake_single_locked<E: ListGetEnd>(
        mut locked: LockedList<Waiter, usize, L, M>,
        notification: Notification,
    ) {
        let Some(mut waiter) = E::get_end(&mut locked) else {
            return;
        };
        waiter.data_mut().notification = Some(notification);
        let waker = waiter.data_mut().waker.take();
        waiter.unlink(|| STATE_OPEN);
        drop(locked);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    #[inline]
    pub fn notify_many(&self, count: usize) {
        if !self.is_empty() {
            self.wake_many(count);
        }
    }

    #[cold]
    #[inline(never)]
    fn wake_many(&self, count: usize) {
        let mut wakers = WakerList::new();
        let mut locked = self.list.lock();
        let mut front = locked.front();
        for _ in 0..count {
            let Some(mut waiter) = front else {
                break;
            };
            waiter.notification = Some(Notification::One);
            wakers.push(unsafe { waiter.waker.take().unwrap_unchecked() });
            front = ListEnd::unlink(waiter, || STATE_OPEN);
            if wakers.is_full() {
                let list = locked.unlock();
                wakers.drain().for_each(Waker::wake);
                if list.is_empty(Relaxed) {
                    return;
                }
                locked = list.lock();
                front = locked.front();
            }
        }
        drop(locked);
        wakers.drain().for_each(Waker::wake);
    }

    #[inline]
    pub fn notify_all(&self) {
        if !self.is_empty() {
            self.notify_all_impl();
        }
    }

    #[cold]
    #[inline(never)]
    fn notify_all_impl(&self) {
        let locked = self.list.lock();
        Self::wake_all(locked, STATE_OPEN, Some(Notification::All));
    }

    #[inline]
    pub fn wait(&self) -> Wait<'_, S, L, M> {
        Wait::new(Node::new(WaitListRef { wait_list: self }))
    }

    #[inline]
    pub fn wait_until<F: FnMut(bool) -> W, W: WakeCondition>(
        &self,
        wake_condition: F,
    ) -> WaitUntil<'_, F, S, L, M> {
        WaitUntil::new(self.wait(), wake_condition)
    }

    #[cold]
    #[inline(never)]
    fn renotify(&self, notification: Notification) {
        match notification {
            Notification::One => self.notify_one(),
            Notification::Last => self.notify_last(),
            _ => unreachable!(),
        }
    }
}

struct WaitListRef<'a, S: Synchronization, L: Linking, M: Mutex> {
    wait_list: &'a WaitList<S, L, M>,
}

as_list!(
    WaitListRef<'a, S: Synchronization, L: Linking, M: Mutex>,
    List<Waiter, usize, L, M>,
    &self.wait_list.list,
);

impl<'a, S: Synchronization, L: Linking, M: Mutex> NodeData<WaitListRef<'a, S, L, M>, usize, L, M>
    for Waiter
{
    fn new_state_if_last_node_on_drop(
        self: Pin<&mut Self>,
        _list: &WaitListRef<'a, S, L, M>,
    ) -> usize {
        STATE_OPEN
    }

    fn on_drop<'list>(
        self: Pin<&mut Self>,
        list: &'list WaitListRef<'a, S, L, M>,
        locked: Option<LockedList<'list, Self, usize, L, M>>,
        state_updated_on_unlink: bool,
    ) {
        if matches!(self.notification, None | Some(Notification::All)) {
            return;
        }
        if let Some(locked) = locked {
            debug_assert!(!state_updated_on_unlink);
            match self.notification {
                Some(Notification::One) => {
                    WaitList::<S, L, M>::wake_single_locked::<GetFront>(locked, Notification::One);
                }
                Some(Notification::Last) => {
                    WaitList::<S, L, M>::wake_single_locked::<GetBack>(locked, Notification::Last);
                }
                _ => {}
            }
        } else {
            list.wait_list.renotify(self.notification.unwrap());
        }
    }
}

struct WakerList {
    wakers: [MaybeUninit<Waker>; 32],
    len: usize,
}

impl WakerList {
    fn new() -> Self {
        Self {
            wakers: unsafe { MaybeUninit::uninit().assume_init() },
            len: 0,
        }
    }
    fn push(&mut self, waker: Waker) {
        self.wakers[self.len].write(waker);
        self.len += 1;
    }
    fn is_full(&self) -> bool {
        self.len == self.wakers.len()
    }
    fn drain(&mut self) -> impl Iterator<Item = Waker> {
        let len = self.len;
        self.len = 0;
        unsafe { assert_unchecked(len <= self.wakers.len()) };
        self.wakers[..len]
            .iter()
            .map(|w| unsafe { w.assume_init_read() })
    }
}

impl Drop for WakerList {
    fn drop(&mut self) {
        self.drain().for_each(drop);
    }
}
