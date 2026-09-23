#![forbid(unsafe_code)]
#[cfg(not(loom))]
use std::sync::atomic::{AtomicUsize, fence};
use std::{
    future::Future,
    marker::PhantomData,
    ops::Deref,
    pin::Pin,
    sync::{
        Arc,
        atomic::Ordering::{AcqRel, Acquire, Relaxed, Release},
    },
    task::{Context, Poll, Waker},
};

#[cfg(loom)]
use loom::sync::atomic::{AtomicUsize, fence};
use maillon::{
    List, ListRef, LockedList, Node, NodeData, NodeState,
    linking::{AtomicEager, Linking},
    list::{Back, End, Front},
    node::NodeRef,
    node_wrapper,
    sync::mutex::DefaultMutex,
};

const STATE_NOTIFIED: usize = 1;
const GENERATION_INCR: usize = 2;
const WAKER_BATCH_SIZE: usize = 32;

#[derive(Clone, Copy)]
enum Notification {
    One,
    Last,
    All,
}

pub struct Notify<L: Linking = AtomicEager> {
    list: List<Waiter, usize, (), L>,
    generation_backup: AtomicUsize,
}

impl<L: Linking> Default for Notify<L> {
    fn default() -> Self {
        Self::new()
    }
}

impl<L: Linking> Notify<L> {
    #[cfg_attr(loom, const_fn::const_fn(cfg(false)))]
    #[inline]
    pub const fn new() -> Self {
        Self {
            list: List::with_state(0),
            generation_backup: AtomicUsize::new(0),
        }
    }

    #[inline(always)]
    fn notify_single<E: End>(&self) {
        self.list.update_state_or_lock_with(
            Relaxed,
            Relaxed,
            |state| state | STATE_NOTIFIED,
            |locked| self.wake_single::<E>(locked),
        );
    }

    fn wake_single<'a, E: End>(&'a self, mut locked: LockedList<'a, Waiter, usize, (), L>) {
        let mut waiter = locked.end::<E>().unwrap();
        waiter.data_mut().notification = Some(if E::IS_FRONT {
            Notification::One
        } else {
            Notification::Last
        });
        let waker = waiter.data_mut().waker.take();
        waiter.unlink(|_, _| self.generation_backup.load(Relaxed));
        drop(locked);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    #[inline]
    pub fn notify_one(&self) {
        self.notify_single::<Front>();
    }

    #[inline]
    pub fn notify_last(&self) {
        self.notify_single::<Back>();
    }

    fn wake_waiters<'a>(&'a self, locked: LockedList<'a, Waiter, usize, (), L>) {
        locked
            .drain(|_| (self.generation_backup.load(Relaxed)).wrapping_add(GENERATION_INCR))
            .wake_all::<WAKER_BATCH_SIZE, _>(|mut waiter, _| {
                waiter.notification = Some(Notification::All);
                waiter.waker.take()
            });
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
        // The generation backup is loaded unconditionally with `unwrap_or`,
        // so it is compiled as a `cmov`. It's important to have the Relaxed
        // load before the Acquire one so they can be both done in parallel.
        let backup = self.generation_backup.load(Relaxed);
        self.list.load_state(Acquire).unwrap_or(backup) & !STATE_NOTIFIED
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
            || cfg!(maillon_notify_fetch_max)
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
    pub fn notified(&self) -> Notified<'_, L> {
        let data = Waiter::new(self.generation());
        Notified(Node::with_data(NotifyRef(self, PhantomData), data))
    }

    #[inline]
    pub fn notified_owned(self: Arc<Self>) -> OwnedNotified<L> {
        let data = Waiter::new(self.generation());
        OwnedNotified(Node::with_data(NotifyRef(self, PhantomData), data))
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

struct NotifyRef<N, L: Linking>(N, PhantomData<L>);
impl<N: Deref<Target = Notify<L>>, L: Linking> ListRef for NotifyRef<N, L> {
    type NodeData = Waiter;
    type ListState = usize;
    type ListData = ();
    type Linking = L;
    type Mutex = DefaultMutex;

    fn as_list(&self) -> &List<Waiter, usize, (), L> {
        &self.0.list
    }
}

impl<N: Deref<Target = Notify<L>>, L: Linking> NodeData<NotifyRef<N, L>> for Waiter {
    fn new_state_if_last_node_on_drop(
        self: Pin<&mut Self>,
        list: &NotifyRef<N, L>,
        _list_data: &mut (),
    ) -> usize {
        debug_assert!(self.notification.is_none());
        list.0.generation_backup.load(Relaxed)
    }

    #[inline(always)]
    fn on_drop<'list>(
        self: Pin<&mut Self>,
        list: &'list NotifyRef<N, L>,
        locked: Option<LockedList<'list, Self, usize, (), L>>,
        state_updated_on_unlink: bool,
    ) {
        if let Some(mut locked) = locked {
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
                        list.0.wake_single::<Front>(locked);
                    }
                    Some(Notification::Last) => {
                        list.0.wake_single::<Back>(locked);
                    }
                    _ => {}
                }
            }
        } else if !self.completed {
            #[cold]
            fn renotify<L: Linking>(notify: &Notify<L>, notification: Option<Notification>) {
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

#[inline(always)]
fn poll_notified<N: Deref<Target = Notify<L>>, L: Linking>(
    node: Pin<&mut Node<NotifyRef<N, L>>>,
    cx: Option<&mut Context<'_>>,
) -> Poll<()> {
    match node.state() {
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
                |waiter, state| {
                    (state == waiter.generation | STATE_NOTIFIED).then_some(state & !STATE_NOTIFIED)
                },
                |mut waiter, _| waiter.completed = true,
                |mut waiter, state| {
                    let completed = match state {
                        Some(state) => {
                            // TODO this assertion is just the negation of the condition above
                            debug_assert!(
                                state & STATE_NOTIFIED == 0
                                    || state & !STATE_NOTIFIED != waiter.generation
                            );
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
                node.unlink(|_, _| unreachable!());
                return Poll::Ready(());
            }
            if let Some(cx) = cx {
                if !matches!(&node.waker, Some(waker) if waker.will_wake(cx.waker())) {
                    node.waker = Some(cx.waker().clone());
                }
            }
            Poll::Pending
        }
    }
}

node_wrapper! {
    pub struct Notified<'a, L: Linking = AtomicEager>(Node<NotifyRef<&'a Notify<L>, L>>);
}

impl<L: Linking> Notified<'_, L> {
    pub fn enable(self: Pin<&mut Self>) -> bool {
        poll_notified(self.node_mut(), None).is_ready()
    }
}

impl<L: Linking> Future for Notified<'_, L> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        poll_notified(self.node_mut(), Some(cx))
    }
}

node_wrapper! {
    pub struct OwnedNotified<L: Linking = AtomicEager>(Node<NotifyRef<Arc<Notify<L>>, L>>);
}

impl<L: Linking> OwnedNotified<L> {
    pub fn enable(self: Pin<&mut Self>) -> bool {
        poll_notified(self.node_mut(), None).is_ready()
    }
}

impl<L: Linking> Future for OwnedNotified<L> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        poll_notified(self.node_mut(), Some(cx))
    }
}

fn main() {}
