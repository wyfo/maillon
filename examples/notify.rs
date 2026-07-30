// #![forbid(unsafe_code)]
#[cfg(not(loom))]
use std::sync::atomic::{AtomicUsize, fence};
use std::{
    ops::Deref,
    pin::Pin,
    sync::{
        Arc,
        atomic::Ordering::{AcqRel, Acquire, Relaxed, Release},
    },
    task::{Context, Poll, Waker},
};

use aiq::{
    List, Node, NodeState, as_list,
    list::{GetBack, GetFront, ListEnd, ListGetEnd, LockedList},
    node::{NodeData, NodeRef},
    sync::DefaultSyncPrimitives,
};
use arrayvec::ArrayVec;
#[cfg(loom)]
use loom::sync::atomic::{AtomicUsize, fence};
use pin_project_lite::pin_project;

const STATE_NOTIFIED: usize = 1;
const GENERATION_INCR: usize = 2;

#[derive(Clone, Copy)]
enum Notification {
    One,
    Last,
    All,
}

#[derive(Default)]
pub struct Notify {
    list: List<Waiter, usize>,
    generation_backup: AtomicUsize,
}

impl Notify {
    #[cfg_attr(loom, const_fn::const_fn(cfg(false)))]
    #[inline]
    pub const fn new() -> Self {
        Self {
            list: List::with_state(0),
            generation_backup: AtomicUsize::new(0),
        }
    }

    #[inline(always)]
    fn notify_single<E: ListGetEnd>(&self, notification: Notification) {
        self.list.update_state_or_lock_with(
            Relaxed,
            Relaxed,
            |state| state | STATE_NOTIFIED,
            |locked| self.wake_single::<E>(notification, locked),
        );
    }

    fn wake_single<'a, E: ListGetEnd>(
        &'a self,
        notification: Notification,
        mut locked: LockedList<'a, Waiter, usize>,
    ) {
        let mut waiter = E::get_end(&mut locked).unwrap();
        waiter.data_mut().notification = Some(notification);
        let waker = waiter.data_mut().waker.take();
        waiter.unlink(|| self.generation_backup.load(Relaxed));
        drop(locked);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    #[inline]
    pub fn notify_one(&self) {
        self.notify_single::<GetFront>(Notification::One);
    }

    #[inline]
    pub fn notify_last(&self) {
        self.notify_single::<GetBack>(Notification::Last);
    }

    fn wake_waiters<'a>(&'a self, locked: LockedList<'a, Waiter, usize>) {
        let mut wakers = ArrayVec::<Waker, 32>::new();
        let next_generation =
            || (self.generation_backup.load(Relaxed)).wrapping_add(GENERATION_INCR);
        locked.drain(next_generation).for_each(
            &mut wakers,
            |wakers, mut waiter| {
                waiter.notification = Some(Notification::All);
                if let Some(waker) = waiter.waker.take() {
                    wakers.push(waker);
                }
                wakers.is_full()
            },
            |wakers| wakers.drain(..).for_each(Waker::wake),
        );
    }

    pub fn notify_waiters(&self) {
        self.list.update_state_or_lock_with(
            Release,
            Relaxed,
            |state| state.wrapping_add(GENERATION_INCR),
            |locked| self.wake_waiters(locked),
        );
    }

    fn generation(&self) -> usize {
        (self.list).load_state_or(Acquire, self.generation_backup.load(Relaxed)) & !STATE_NOTIFIED
    }

    // TODO
    /// Publishes `generation` into [`Self::generation_backup`], so that it stays available
    /// while the tail word holds a node pointer instead of the state.
    ///
    /// Returns `false` if a *newer* generation had already been published, meaning the
    /// caller has been notified in the meantime and must complete instead of waiting.
    fn store_generation_backup(&self, generation: usize) -> bool {
        debug_assert!(generation & STATE_NOTIFIED == 0);
        if cfg!(all(target_arch = "aarch64", target_pointer_width = "64"))
            || cfg!(aiq_notify_fetch_max)
        {
            let backup = self.generation_backup.load(Relaxed);
            let stored = backup == generation
                || (backup < generation
                    && self.generation_backup.fetch_max(generation, Release) <= generation);
            if !stored {
                fence(Acquire);
            }
            stored
        } else {
            // TODO 32-bit can't use a raw comparison, but the codegen is almost equivalent
            // TODO backup - generation gives better codegen than generation - backup
            let is_old = |backup: usize| (backup.wrapping_sub(generation) as isize) < 0;
            let mut backup = self.generation_backup.load(Relaxed);
            loop {
                if backup == generation {
                    return true;
                } else if !is_old(backup) {
                    fence(Acquire);
                    return false;
                }
                match (self.generation_backup)
                    .compare_exchange_weak(backup, generation, Release, Relaxed)
                {
                    Ok(_) => return true,
                    Err(b) => backup = b,
                }
            }
        }
    }

    #[inline]
    pub fn notified(&self) -> Notified<'_> {
        Notified {
            inner: NotifiedInner {
                node: Node::with_data(NotifyRef(self), Waiter::new(self.generation())),
            },
        }
    }

    #[inline]
    pub fn notified_owned(self: Arc<Self>) -> OwnedNotified {
        let generation = self.generation();
        OwnedNotified {
            inner: NotifiedInner {
                node: Node::with_data(NotifyRef(self), Waiter::new(generation)),
            },
        }
    }
}

struct Waiter {
    generation: usize,
    notification: Option<Notification>,
    completed: bool,
    waker: Option<Waker>,
}

impl Waiter {
    fn new(generation: usize) -> Self {
        Self {
            generation,
            notification: None,
            completed: false,
            waker: None,
        }
    }
}

struct NotifyRef<N>(N);
as_list!(NotifyRef<N: Deref<Target=Notify>>, List<Waiter, usize>, &self.0.list);

impl<N: Deref<Target = Notify>> NodeData<NotifyRef<N>, usize> for Waiter {
    fn new_state_if_last_node_on_drop(self: Pin<&mut Self>, list: &NotifyRef<N>) -> usize {
        list.0.generation_backup.load(Relaxed)
    }

    #[inline(always)]
    fn on_drop<'list>(
        self: Pin<&mut Self>,
        list: &'list NotifyRef<N>,
        locked: Option<LockedList<'list, Self, usize, DefaultSyncPrimitives>>,
        state_updated_on_unlink: bool,
    ) {
        if let Some(locked) = locked {
            debug_assert!(!self.completed);
            debug_assert!(!state_updated_on_unlink || self.notification.is_none());
            if matches!(
                self.notification,
                Some(Notification::One | Notification::Last)
            ) && locked
                .try_update_state(Release, Relaxed, |state| Some(state | STATE_NOTIFIED))
                .is_err()
            {
                match self.notification {
                    Some(Notification::One) => {
                        list.0.wake_single::<GetFront>(Notification::One, locked);
                    }
                    Some(Notification::Last) => {
                        list.0.wake_single::<GetBack>(Notification::Last, locked);
                    }
                    _ => {}
                }
            }
        } else if !self.completed {
            #[cold]
            fn renotify(notify: &Notify, notification: Option<Notification>) {
                match notification {
                    Some(Notification::One) => notify.notify_one(),
                    Some(Notification::Last) => notify.notify_last(),
                    _ => {}
                }
            }
            renotify(&list.0, self.notification);
        }
    }
}

pin_project! {
    struct NotifiedInner<N: Deref<Target = Notify>> {
        #[pin]
        node: Node<NotifyRef<N>, Waiter, usize>,
    }
}

impl<N: Deref<Target = Notify>> NotifiedInner<N> {
    #[inline(always)]
    fn poll_notified(self: Pin<&mut Self>, cx: Option<&mut Context<'_>>) -> Poll<()> {
        match self.project().node.state() {
            NodeState::Unlinked(mut node) => {
                if node.notification.is_some() {
                    node.completed = true;
                }
                if node.completed {
                    return Poll::Ready(());
                }
                let notify = &*node.list().0;
                match node.try_update_state_or_push_back_with(
                    AcqRel,  // TODO Acquire for successful notification, Release for generation_backup CAS
                    Acquire, // TODO Acquire for generation
                    |_, state| (state & STATE_NOTIFIED != 0).then_some(state & !STATE_NOTIFIED),
                    |mut waiter, _| waiter.completed = true,
                    |mut waiter, state| {
                        let completed = match state {
                            Some(state) => {
                                waiter.generation != state || !notify.store_generation_backup(state)
                            }
                            None => waiter.generation != notify.generation_backup.load(Relaxed),
                        };
                        if completed {
                            waiter.completed = true;
                            return false;
                        }
                        if let Some(cx) = cx.as_ref() {
                            waiter.waker.get_or_insert_with(|| cx.waker().clone());
                        }
                        true
                    },
                ) {
                    Ok(_) | Err(false) => Poll::Ready(()),
                    Err(true) => Poll::Pending,
                }
            }
            NodeState::Linked(mut node) => {
                if node.list().0.generation() != node.generation {
                    // TODO if generation is different, the node must be in a drain list
                    node.completed = true;
                    node.unlink(|| unreachable!());
                    return Poll::Ready(());
                }
                if let Some(cx) = cx
                    && (node.waker.as_ref()).is_none_or(|waker| !waker.will_wake(cx.waker()))
                {
                    node.waker = Some(cx.waker().clone());
                }
                Poll::Pending
            }
        }
    }
}

pin_project! {
    pub struct Notified<'a> {
        #[pin]
        inner: NotifiedInner<&'a Notify>
    }
}

impl Notified<'_> {
    pub fn enable(self: Pin<&mut Self>) -> bool {
        self.project().inner.poll_notified(None).is_ready()
    }
}

impl Future for Notified<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll_notified(Some(cx))
    }
}

pin_project! {
    pub struct OwnedNotified {
        #[pin]
        inner: NotifiedInner<Arc<Notify>>
    }
}

impl OwnedNotified {
    pub fn enable(self: Pin<&mut Self>) -> bool {
        self.project().inner.poll_notified(None).is_ready()
    }
}

impl Future for OwnedNotified {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll_notified(Some(cx))
    }
}

fn main() {}

#[unsafe(no_mangle)]
fn plop(_notified: Notified) {}
