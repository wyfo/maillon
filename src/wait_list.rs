#[cfg(feature = "alloc")]
extern crate alloc;

#[cfg(feature = "alloc")]
use alloc::sync::Arc;
use core::{
    future::Future,
    hint::assert_unchecked,
    marker::PhantomData,
    mem::MaybeUninit,
    ops::Deref,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use crate::{
    Node, NodeState, Queue,
    queue::LockedQueue,
    queue_ref,
    sync::{DefaultSyncPrimitives, SyncPrimitives},
};

const EMPTY: usize = 0;
const CLOSED: usize = 1;

#[derive(Debug, Default)]
struct Waiter {
    waker: Option<Waker>,
    notification: Option<Notification>,
}

#[derive(Debug, Clone, Copy)]
enum Notification {
    One,
    Last,
}

pub struct WaitQueue<SP: SyncPrimitives = DefaultSyncPrimitives> {
    queue: Queue<Waiter, usize, SP>,
}

impl<SP: SyncPrimitives> Default for WaitQueue<SP> {
    fn default() -> Self {
        Self::new()
    }
}

impl<SP: SyncPrimitives> WaitQueue<SP> {
    #[cfg_attr(loom, const_fn::const_fn(cfg(false)))]
    #[inline]
    pub const fn new() -> Self {
        Self {
            queue: Queue::with_state_const(EMPTY),
        }
    }

    pub fn is_closed(&self) -> bool {
        self.queue.state().is_some_and(|s| s != EMPTY)
    }

    pub fn close(&self) {
        if let Some(locked) = self.queue.fetch_update_state_or_lock(|_| CLOSED) {
            drain_queue::<CLOSED, SP>(locked);
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    #[inline]
    pub fn notify_one(&self) {
        self.queue.is_empty_or_locked(|mut locked| {
            let mut waiter = unsafe { locked.dequeue().unwrap_unchecked() };
            waiter.with_data_mut(|mut w| w.notification = Some(Notification::One));
            let waker = waiter.with_data_mut(|mut w| unsafe { w.waker.take().unwrap_unchecked() });
            drop(waiter);
            drop(locked);
            waker.wake();
        });
    }

    #[inline]
    pub fn notify_last(&self) {
        self.queue.is_empty_or_locked(|mut locked| {
            let mut waiter = unsafe { locked.pop().unwrap_unchecked() };
            waiter.with_data_mut(|mut w| w.notification = Some(Notification::Last));
            let waker = waiter.with_data_mut(|mut w| unsafe { w.waker.take().unwrap_unchecked() });
            drop(waiter);
            drop(locked);
            waker.wake();
        });
    }

    #[inline]
    pub fn notify_many(&self, count: usize) {
        self.queue
            .is_empty_or_locked(|locked| notify_many(locked, count));
    }

    #[inline]
    pub fn notify_many_const<const COUNT: usize>(&self) {
        if COUNT == 1 {
            self.notify_one();
        } else {
            self.queue
                .is_empty_or_locked(|locked| notify_many(locked, COUNT));
        }
    }

    #[inline]
    pub fn notify_all(&self) {
        self.queue.is_empty_or_locked(drain_queue::<EMPTY, SP>);
    }

    #[inline]
    pub fn wait(&self) -> Wait<&Self, SP> {
        Wait {
            node: Node::new(WaitQueueRef {
                wait_queue: self,
                _sync_primitives: PhantomData,
            }),
        }
    }

    #[cfg(feature = "alloc")]
    #[inline]
    pub fn wait_owned(self: Arc<Self>) -> Wait<Arc<Self>, SP> {
        Wait {
            node: Node::new(WaitQueueRef {
                wait_queue: self,
                _sync_primitives: PhantomData,
            }),
        }
    }

    #[inline]
    pub fn wait_if<P: WaitIfPredicate>(&self, predicate: P) -> WaitIf<&Self, P, SP> {
        WaitIf {
            wait: self.wait(),
            predicate: Some(predicate),
        }
    }

    #[cfg(feature = "alloc")]
    #[inline]
    pub fn wait_if_owned<P: WaitIfPredicate>(
        self: Arc<Self>,
        predicate: P,
    ) -> WaitIf<Arc<Self>, P, SP> {
        WaitIf {
            wait: self.wait_owned(),
            predicate: Some(predicate),
        }
    }

    #[inline]
    pub fn wait_until<P: WaitUntilPredicate>(&self, predicate: P) -> WaitUntil<&Self, P, SP> {
        WaitUntil {
            wait: self.wait(),
            predicate,
        }
    }

    #[cfg(feature = "alloc")]
    #[inline]
    pub fn wait_until_owned<P: WaitUntilPredicate>(
        self: Arc<Self>,
        predicate: P,
    ) -> WaitUntil<Arc<Self>, P, SP> {
        WaitUntil {
            wait: self.wait_owned(),
            predicate,
        }
    }
}

struct WaitQueueRef<L, SP> {
    wait_queue: L,
    _sync_primitives: PhantomData<SP>,
}

unsafe impl<L: Send, SP> Send for WaitQueueRef<L, SP> {}
unsafe impl<L: Sync, SP> Sync for WaitQueueRef<L, SP> {}

queue_ref!(WaitQueueRef<L: Deref<Target = WaitQueue<SP>>, SP: SyncPrimitives>, NodeData = Waiter, State = usize, SyncPrimitives = SP, &self.wait_queue.queue, |q: &WaitQueueRef<L, SP>, w: &mut Waiter| match w.notification {
    Some(Notification::One) => q.wait_queue.notify_one(),
    Some(Notification::Last) => q.wait_queue.notify_last(),
    None => {}
});

pub struct Wait<L: Deref<Target = WaitQueue<SP>>, SP: SyncPrimitives = DefaultSyncPrimitives> {
    node: Node<WaitQueueRef<L, SP>>,
}

impl<L: Deref<Target = WaitQueue<SP>>, SP: SyncPrimitives> Wait<L, SP> {
    #[cold]
    pub fn poll_wait(self: Pin<&mut Self>, cx: &mut Context<'_>, requeue: bool) -> Poll<()> {
        let mut waiter = match unsafe { self.map_unchecked_mut(|this| &mut this.node) }.state() {
            NodeState::Unqueued(waiter) => waiter,
            NodeState::Queued(mut waiter) => {
                waiter.with_data_mut(|mut waiter| {
                    if (waiter.waker.as_ref()).is_none_or(|w| !w.will_wake(cx.waker())) {
                        waiter.waker = Some(cx.waker().clone());
                    }
                });
                return Poll::Pending;
            }
            NodeState::Dequeued(waiter) if requeue => waiter.reset(),
            NodeState::Dequeued(mut waiter) => {
                // remove the notification, so destructor don't trigger a new notification
                waiter.with_data_mut(|mut waiter| waiter.notification.take());
                return Poll::Ready(());
            }
        };
        waiter.with_data_mut(|mut waiter| {
            waiter.waker = Some(cx.waker().clone());
        });
        match waiter.try_enqueue_with_queue_state(|s| s.is_none_or(|s| s == EMPTY)) {
            Ok(_) => Poll::Pending,
            Err(_) => Poll::Ready(()),
        }
    }
}

impl<L: Deref<Target = WaitQueue<SP>>, SP: SyncPrimitives> Future for Wait<L, SP> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.poll_wait(cx, false)
    }
}

pub trait WaitIfPredicate {
    fn check(self) -> bool;
}

impl<F: FnOnce() -> bool> WaitIfPredicate for F {
    fn check(self) -> bool {
        self()
    }
}

pub struct WaitIf<
    L: Deref<Target = WaitQueue<SP>>,
    P: WaitIfPredicate,
    SP: SyncPrimitives = DefaultSyncPrimitives,
> {
    wait: Wait<L, SP>,
    predicate: Option<P>,
}

impl<L: Deref<Target = WaitQueue<SP>>, P: WaitIfPredicate, SP: SyncPrimitives> Future
    for WaitIf<L, P, SP>
{
    type Output = ();

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        match unsafe { Pin::new_unchecked(&mut this.wait) }.poll_wait(cx, false) {
            Poll::Pending if this.predicate.take().is_some_and(|p| !p.check()) => Poll::Ready(()),
            poll => poll,
        }
    }
}

pub trait WaitUntilPredicate {
    type Output;
    fn check(&mut self) -> Option<Self::Output>;
}

impl<F: FnMut() -> Option<T>, T> WaitUntilPredicate for F {
    type Output = T;

    fn check(&mut self) -> Option<Self::Output> {
        self()
    }
}

pub struct WaitUntil<
    L: Deref<Target = WaitQueue<SP>>,
    P: WaitUntilPredicate,
    SP: SyncPrimitives = DefaultSyncPrimitives,
> {
    wait: Wait<L, SP>,
    predicate: P,
}

impl<L: Deref<Target = WaitQueue<SP>>, P: WaitUntilPredicate, SP: SyncPrimitives>
    WaitUntil<L, P, SP>
{
    #[cold]
    unsafe fn poll_cold(&mut self, cx: &mut Context<'_>) -> Poll<P::Output> {
        let is_closed = unsafe { Pin::new_unchecked(&mut self.wait) }
            .poll_wait(cx, true)
            .is_ready();
        match self.predicate.check() {
            Some(res) => Poll::Ready(res),
            None if is_closed => panic!("wait queue is closed but predicate didn't return `Some`"),
            None => Poll::Pending,
        }
    }
}

impl<L: Deref<Target = WaitQueue<SP>>, P: WaitUntilPredicate, SP: SyncPrimitives> Future
    for WaitUntil<L, P, SP>
{
    type Output = P::Output;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        match this.predicate.check() {
            Some(res) => Poll::Ready(res),
            None => unsafe { this.poll_cold(cx) },
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

fn notify_many<SP: SyncPrimitives>(mut locked: LockedQueue<Waiter, usize, SP>, count: usize) {
    let mut wakers = WakerList::new();
    for _ in 0..count {
        let Some(mut waiter) = locked.dequeue() else {
            drop(locked);
            break;
        };
        waiter.with_data_mut(|mut w| w.notification = Some(Notification::One));
        wakers.push(waiter.with_data_mut(|mut w| unsafe { w.waker.take().unwrap_unchecked() }));
        drop(waiter);
        if wakers.is_full() {
            let queue = locked.unlock();
            wakers.drain().for_each(Waker::wake);
            match queue.is_empty_or_lock() {
                Some(l) => locked = l,
                None => break,
            };
        }
    }
    wakers.drain().for_each(Waker::wake);
}

fn drain_queue<const STATE: usize, SP: SyncPrimitives>(locked: LockedQueue<Waiter, usize, SP>) {
    let mut wakers = WakerList::new();
    locked.drain_try_set_state(STATE).for_each(
        &mut wakers,
        |wakers, mut waker| {
            wakers.push(unsafe { waker.waker.take().unwrap_unchecked() });
            wakers.is_full()
        },
        |wakers| wakers.drain().for_each(Waker::wake),
    );
}

#[unsafe(no_mangle)]
fn plop(q: &WaitQueue) {
    q.notify_many_const::<32>();
}
