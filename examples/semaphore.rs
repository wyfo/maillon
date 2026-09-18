#![forbid(unsafe_code)]
use std::{
    cmp::min,
    future::Future,
    mem,
    pin::Pin,
    sync::{
        Arc,
        atomic::Ordering::{Acquire, Relaxed, Release},
    },
    task::{Context, Poll, Waker},
};

use maillon::{
    List, ListRef, LockedList, Node, NodeData, NodeState, WakerBatch,
    linking::{AtomicEager, Linking},
    node_wrapper,
    sync::mutex::DefaultMutex,
};

const CLOSED: usize = 1;
const PERMIT_SHIFT: usize = 1;
const WAKER_BATCH_SIZE: usize = 32;

pub struct Semaphore<L: Linking = AtomicEager>(List<Waiter, usize, (), L>);

impl<L: Linking> Default for Semaphore<L> {
    fn default() -> Self {
        Self::new(0)
    }
}

impl<L: Linking> Semaphore<L> {
    pub const MAX_PERMITS: usize = usize::MAX >> 3;

    #[inline(always)]
    const fn check_add_permits(state: usize, add: usize) -> usize {
        let current = state >> PERMIT_SHIFT;
        assert!(add <= Self::MAX_PERMITS - current, "permits overflow");
        state + (add << PERMIT_SHIFT)
    }

    #[inline(always)]
    const fn check_acquire_permits(state: usize, acquire: u32) -> Option<usize> {
        if state & CLOSED != 0 {
            return None;
        }
        state.checked_sub((acquire as usize) << PERMIT_SHIFT)
    }

    #[cfg_attr(loom, const_fn::const_fn(cfg(false)))]
    #[inline]
    pub const fn new(permits: usize) -> Self {
        Self(List::with_state(Self::check_add_permits(0, permits)))
    }

    #[inline]
    pub fn available_permits(&self) -> usize {
        self.0.load_state(Relaxed).unwrap_or(0) >> PERMIT_SHIFT
    }

    #[inline]
    pub fn add_permits(&self, permits: usize) {
        if permits == 0 {
            return;
        }
        let add_permits = |state| Self::check_add_permits(state, permits);
        let fallback = |locked| self.add_permits_locked(permits, locked);
        (self.0).update_state_or_lock_with(Release, Relaxed, add_permits, fallback);
    }

    fn add_permits_locked<'a>(
        &'a self,
        mut permits: usize,
        mut locked: LockedList<'a, Waiter, usize, (), L>,
    ) {
        assert!(!locked.is_empty(Relaxed));
        let mut wakers = WakerBatch::<WAKER_BATCH_SIZE>::new();
        let mut waiter = locked.front().unwrap();
        loop {
            if waiter.permits_remaining as usize > permits {
                waiter.permits_remaining -= permits as u32;
                break;
            }
            permits -= waiter.permits_remaining as usize;
            waiter.permits_remaining = 0;
            wakers.push(waiter.waker.take().unwrap());
            match waiter.unlink(|_, _| permits << PERMIT_SHIFT) {
                Some(w) if permits > 0 => waiter = w,
                _ => break,
            }
            if wakers.is_full() {
                drop(locked);
                wakers.wake_all();
                match self.0.update_state_or_lock(Release, Relaxed, |state| {
                    Self::check_add_permits(state, permits)
                }) {
                    Ok(_) => return,
                    Err(l) => locked = l,
                }
                waiter = locked.front().unwrap();
            }
        }
        drop(locked);
        wakers.wake_all();
    }

    #[inline]
    pub fn forget_permits(&self, permits: usize) -> usize {
        if permits == 0 {
            return 0;
        }
        let state = (self.0)
            .try_update_state(Relaxed, Relaxed, |state| {
                Some(state.wrapping_sub(permits << PERMIT_SHIFT))
            })
            .unwrap_or(0);
        min(permits, state >> PERMIT_SHIFT)
    }

    #[inline]
    pub async fn acquire(&self) -> Result<SemaphorePermit<'_, L>, AcquireError> {
        self.acquire_many(1).await
    }

    #[inline]
    pub async fn acquire_many(&self, permits: u32) -> Result<SemaphorePermit<'_, L>, AcquireError> {
        let acquire = |state| Self::check_acquire_permits(state, permits as _);
        if self.0.try_update_state(Acquire, Relaxed, acquire).is_err() {
            AcquireFuture(Node::with_data(SemaphoreRef(self), Waiter::new(permits))).await?;
        }
        Ok(SemaphorePermit { sem: self, permits })
    }

    #[inline]
    pub fn try_acquire(&self) -> Result<SemaphorePermit<'_, L>, TryAcquireError> {
        self.try_acquire_many(1)
    }

    #[inline]
    pub fn try_acquire_many(
        &self,
        permits: u32,
    ) -> Result<SemaphorePermit<'_, L>, TryAcquireError> {
        let acquire = |state| Self::check_acquire_permits(state, permits);
        match self.0.try_update_state(Acquire, Relaxed, acquire) {
            Ok(_) => Ok(SemaphorePermit { sem: self, permits }),
            Err(Some(state)) if state & CLOSED != 0 => Err(TryAcquireError::Closed),
            Err(_) => Err(TryAcquireError::NoPermits),
        }
    }

    #[inline]
    pub async fn acquire_owned(self: Arc<Self>) -> Result<OwnedSemaphorePermit<L>, AcquireError> {
        self.acquire_many_owned(1).await
    }

    #[inline]
    pub async fn acquire_many_owned(
        self: Arc<Self>,
        permits: u32,
    ) -> Result<OwnedSemaphorePermit<L>, AcquireError> {
        mem::forget(self.acquire_many(permits).await?);
        Ok(OwnedSemaphorePermit { sem: self, permits })
    }

    #[inline]
    pub fn try_acquire_owned(self: Arc<Self>) -> Result<OwnedSemaphorePermit<L>, TryAcquireError> {
        self.try_acquire_many_owned(1)
    }

    #[inline]
    pub fn try_acquire_many_owned(
        self: Arc<Self>,
        permits: u32,
    ) -> Result<OwnedSemaphorePermit<L>, TryAcquireError> {
        mem::forget(self.try_acquire_many(permits)?);
        Ok(OwnedSemaphorePermit { sem: self, permits })
    }

    pub fn close(&self) {
        if let Err(locked) = (self.0).update_state_or_lock(Release, Relaxed, |state| state | CLOSED)
        {
            locked
                .drain(|_| CLOSED)
                .wake_all::<WAKER_BATCH_SIZE, _>(|mut waiter, _| waiter.waker.take());
        }
    }

    #[inline]
    pub fn is_closed(&self) -> bool {
        matches!(self.0.load_state(Acquire), Some(state) if state & CLOSED != 0)
    }
}

struct Waiter {
    permits_total: u32,
    permits_remaining: u32,
    waker: Option<Waker>,
}

impl Waiter {
    fn new(permits: u32) -> Self {
        Self {
            permits_total: permits,
            permits_remaining: permits,
            waker: None,
        }
    }
}

struct SemaphoreRef<'a, L: Linking>(&'a Semaphore<L>);
impl<L: Linking> ListRef for SemaphoreRef<'_, L> {
    type NodeData = Waiter;
    type ListState = usize;
    type ListData = ();
    type Linking = L;
    type Mutex = DefaultMutex;

    fn as_list(&self) -> &List<Waiter, usize, (), L> {
        &self.0.0
    }
}

impl<'a, L: Linking> NodeData<SemaphoreRef<'a, L>> for Waiter {
    fn new_state_if_last_node_on_drop(
        self: Pin<&mut Self>,
        _list: &SemaphoreRef<'a, L>,
        _list_data: &mut (),
    ) -> usize {
        ((self.permits_total - self.permits_remaining) as usize) << PERMIT_SHIFT
    }

    #[inline]
    fn on_drop<'list>(
        self: Pin<&mut Self>,
        list: &'list SemaphoreRef<'a, L>,
        locked: Option<LockedList<'list, Self, usize, (), L>>,
        state_updated_on_unlink: bool,
    ) {
        if state_updated_on_unlink || self.permits_remaining == 0 {
            return;
        }
        let acquired = self.permits_total - self.permits_remaining;
        if let Some(mut locked) = locked {
            let add_permits = |state| Some(Semaphore::<L>::check_add_permits(state, acquired as _));
            if acquired == 0 || (locked.try_update_state(Relaxed, Relaxed, add_permits)).is_ok() {
                return;
            };
            list.0.add_permits_locked(acquired as _, locked);
        } else {
            #[cold]
            fn add_permits_cold<L: Linking>(semaphore: &Semaphore<L>, permits: usize) {
                if permits == 0 {
                    return;
                }
                let add_permits = |state| Semaphore::<L>::check_add_permits(state, permits);
                let fallback = |locked| semaphore.add_permits_locked(permits, locked);
                (semaphore.0).update_state_or_lock_with(Relaxed, Relaxed, add_permits, fallback);
            }
            add_permits_cold(list.0, acquired as _);
        }
    }
}

#[derive(Debug)]
pub struct AcquireError(());
#[derive(Debug)]
pub enum TryAcquireError {
    Closed,
    NoPermits,
}

node_wrapper! {
    struct AcquireFuture<'a, L: Linking>(Node<SemaphoreRef<'a, L>>);
}

impl<L: Linking> Future for AcquireFuture<'_, L> {
    type Output = Result<(), AcquireError>;

    #[cold]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.node_mut().state() {
            NodeState::Unlinked(node) => {
                if node.permits_remaining == 0 {
                    return Poll::Ready(Ok(()));
                }
                match node.try_update_state_or_push_back_with(
                    Acquire, // TODO Acquire for close
                    Acquire,
                    |waiter, state| {
                        Semaphore::<L>::check_acquire_permits(state, waiter.permits_total)
                    },
                    |mut waiter, _| waiter.permits_remaining = 0,
                    |mut waiter, state| {
                        if matches!(state, Some(s) if s & CLOSED != 0) {
                            return false;
                        }
                        waiter.permits_remaining =
                            waiter.permits_total - (state.unwrap_or(0) as u32 >> PERMIT_SHIFT);
                        waiter.waker.get_or_insert_with(|| cx.waker().clone());
                        true
                    },
                ) {
                    Ok(_) => Poll::Ready(Ok(())),
                    Err(true) => Poll::Pending,
                    Err(false) => Poll::Ready(Err(AcquireError(()))),
                }
            }
            NodeState::Linked(mut node) => {
                debug_assert_ne!(node.permits_remaining, 0);
                if !node.waker.as_ref().unwrap().will_wake(cx.waker()) {
                    node.waker = Some(cx.waker().clone());
                }
                Poll::Pending
            }
        }
    }
}

pub struct SemaphorePermit<'a, L: Linking = AtomicEager> {
    sem: &'a Semaphore<L>,
    permits: u32,
}

impl<L: Linking> SemaphorePermit<'_, L> {
    pub fn forget(mut self) {
        self.permits = 0;
    }

    pub fn merge(&mut self, mut other: Self) {
        assert!(
            std::ptr::eq(self.sem, other.sem),
            "merging permits from different semaphore instances"
        );
        self.permits += other.permits;
        other.permits = 0;
    }

    pub fn split(&mut self, n: usize) -> Option<Self> {
        let n = u32::try_from(n).ok()?;

        if n > self.permits {
            return None;
        }

        self.permits -= n;

        Some(Self {
            sem: self.sem,
            permits: n,
        })
    }

    pub fn num_permits(&self) -> usize {
        self.permits as usize
    }
}

impl<L: Linking> Drop for SemaphorePermit<'_, L> {
    fn drop(&mut self) {
        self.sem.add_permits(self.permits as _);
    }
}

pub struct OwnedSemaphorePermit<L: Linking = AtomicEager> {
    sem: Arc<Semaphore<L>>,
    permits: u32,
}

impl<L: Linking> OwnedSemaphorePermit<L> {
    pub fn forget(mut self) {
        self.permits = 0;
    }

    pub fn merge(&mut self, mut other: Self) {
        assert!(
            Arc::ptr_eq(&self.sem, &other.sem),
            "merging permits from different semaphore instances"
        );
        self.permits += other.permits;
        other.permits = 0;
    }

    pub fn split(&mut self, n: usize) -> Option<Self> {
        let n = u32::try_from(n).ok()?;

        if n > self.permits {
            return None;
        }

        self.permits -= n;

        Some(Self {
            sem: self.sem.clone(),
            permits: n,
        })
    }

    pub fn semaphore(&self) -> &Arc<Semaphore<L>> {
        &self.sem
    }

    pub fn num_permits(&self) -> usize {
        self.permits as usize
    }
}

impl<L: Linking> Drop for OwnedSemaphorePermit<L> {
    fn drop(&mut self) {
        self.sem.add_permits(self.permits as _);
    }
}

fn main() {}
