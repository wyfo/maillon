#[cfg(feature = "alloc")]
extern crate alloc;

use core::{hint, marker::PhantomData, mem::ManuallyDrop, ops::Deref, ptr, ptr::NonNull};

use crate::{
    loom::{
        AtomicPtrExt,
        sync::atomic::{AtomicPtr, Ordering, Ordering::*, fence},
    },
    node::{NULL, NodeLink, node_ref},
    sync::{DefaultSyncPrimitives, SyncPrimitives, mutex::Mutex, parker::Parker},
};

mod drain;
pub(crate) mod state;

pub use drain::*;
pub use state::*;

use crate::node::NodeRef;

type MutexGuard<'a, SP> = <<SP as SyncPrimitives>::Mutex as Mutex>::Guard<'a>;

const HEAD_MARKER: *mut NodeLink = ptr::without_provenance_mut(1);

pub struct List<T, S: ListState = (), SP: SyncPrimitives = DefaultSyncPrimitives> {
    tail: AtomicPtr<Tail<S>>,
    head: AtomicPtr<NodeLink>,
    mutex: SP::Mutex,
    parker: SP::Parker,
    _node_data: PhantomData<T>,
}

unsafe impl<T: Send, S: ListState + Send, SP: SyncPrimitives> Send for List<T, S, SP>
where
    SP::Mutex: Send,
    SP::Parker: Send,
{
}
unsafe impl<T: Send, S: ListState + Send, SP: SyncPrimitives> Sync for List<T, S, SP>
where
    SP::Mutex: Sync,
    SP::Parker: Sync,
{
}

impl<T, S: ListState, SP: SyncPrimitives> List<T, S, SP> {
    #[cfg_attr(loom, const_fn::const_fn(cfg(false)))]
    const fn new_impl(tail: *mut Tail<S>) -> Self {
        Self {
            tail: AtomicPtr::new(tail),
            head: AtomicPtr::new(NULL),
            #[cfg(not(loom))]
            mutex: SP::Mutex::INIT,
            #[cfg(loom)]
            mutex: SP::Mutex::new(),
            #[cfg(not(loom))]
            parker: SP::Parker::INIT,
            #[cfg(loom)]
            parker: SP::Parker::new(),
            _node_data: PhantomData,
        }
    }

    #[cfg_attr(loom, const_fn::const_fn(cfg(false)))]
    #[inline]
    pub const fn new() -> Self {
        Self::new_impl(ptr::null_mut())
    }

    #[inline(always)]
    fn tail(&self) -> Option<NonNull<NodeLink>> {
        self.tail.load(Acquire).ptr()
    }

    #[inline]
    pub fn is_empty(&self, order: Ordering) -> bool {
        self.tail.load(order).ptr().is_none()
    }

    #[inline]
    pub fn is_empty_rmw(&self, order: Ordering) -> bool {
        self.tail.fetch_byte_add(0, order).ptr().is_none()
    }

    #[inline]
    pub fn lock(&self) -> LockedList<'_, T, S, SP> {
        LockedList {
            queue: self,
            guard: ManuallyDrop::new(self.mutex.lock()),
            _not_send: PhantomData,
        }
    }

    pub(crate) unsafe fn push_back(
        &self,
        mut node: NonNull<NodeLink>,
        set_order: Ordering,
        fetch_order: Ordering,
        mut f: impl FnMut(S) -> Option<S>,
        mut on_push_back: impl FnMut(Option<S>) -> bool,
        on_pushed: impl FnOnce(),
    ) -> Result<S, bool> {
        let set_order = match set_order {
            Relaxed | Acquire | Release | AcqRel => AcqRel,
            _ => SeqCst, // `Ordering` is `#[non_exhaustive]`
        };
        let mut tail = self.tail.load(fetch_order);
        let prev = loop {
            let (new_tail, prev) = match S::tail_to_enum(tail) {
                StateOrPtr::State(state) if let Some(new_state) = f(state) => {
                    (new_state.into_tail(), NULL)
                }
                state_or_ptr if !on_push_back(state_or_ptr.state()) => {
                    unsafe { node.as_mut().prev.store_mut(NULL) };
                    return Err(false);
                }
                StateOrPtr::State(_) => (node.into_tail(), HEAD_MARKER),
                StateOrPtr::Ptr(prev) => (node.into_tail(), prev.as_ptr()),
            };
            unsafe { node.as_mut().prev.store_mut(prev) };
            match (self.tail).compare_exchange_weak(tail, new_tail, set_order, fetch_order) {
                Ok(_) => break prev,
                Err(t) => tail = t,
            }
        };
        let prev_next = NonNull::from(match prev {
            NULL => return Ok(unsafe { tail.state().unwrap_unchecked() }),
            HEAD_MARKER => &self.head,
            _ => unsafe { &(*prev).next },
        });
        on_pushed();
        if SP::Parker::NEVER_BLOCKS {
            unsafe { prev_next.as_ref() }.store(node.as_ptr(), Release);
        } else if unsafe { !(prev_next.as_ref().swap(node.as_ptr(), Release)).is_null() } {
            self.unpark();
        }
        Err(true)
    }

    #[cold]
    #[inline(never)]
    fn unpark(&self) {
        self.parker.unpark();
    }
}

impl<T, SP: SyncPrimitives> List<T, usize, SP> {
    #[cfg_attr(loom, const_fn::const_fn(cfg(false)))]
    #[inline]
    pub const fn with_state(state: usize) -> Self {
        Self::new_impl(state_to_ptr(state))
    }

    #[inline]
    pub fn load_state(&self, order: Ordering) -> Option<usize> {
        self.tail.load(order).state()
    }

    #[inline]
    pub fn load_state_or(&self, order: Ordering, default: usize) -> usize {
        usize::tail_to_state_or(self.tail.load(order), default)
    }

    #[inline]
    pub fn load_state_rmw(&self, order: Ordering) -> Option<usize> {
        self.tail.fetch_byte_add(0, order).state()
    }

    #[inline]
    pub fn compare_exchange_state(
        &self,
        current: usize,
        new: usize,
        success: Ordering,
        failure: Ordering,
    ) -> Result<usize, Option<usize>> {
        match (self.tail).compare_exchange(current.into_tail(), new.into_tail(), success, failure) {
            Ok(_) => Ok(current),
            Err(ptr) => Err(ptr.state()),
        }
    }

    #[inline]
    pub fn try_update_state<F: FnMut(usize) -> Option<usize>>(
        &self,
        set_order: Ordering,
        fetch_order: Ordering,
        mut f: F,
    ) -> Result<usize, Option<usize>> {
        let mut tail = self.tail.load(fetch_order);
        while let Some(state) = tail.state() {
            let Some(new_state) = f(state) else {
                return Err(Some(state));
            };
            let new_tail = new_state.into_tail();
            match (self.tail).compare_exchange_weak(tail, new_tail, set_order, fetch_order) {
                Ok(_) => return Ok(state),
                Err(ptr) => tail = ptr,
            }
        }
        Err(None)
    }

    #[inline]
    pub fn update_state_or_lock<F: FnMut(usize) -> usize>(
        &self,
        set_order: Ordering,
        fetch_order: Ordering,
        mut f: F,
    ) -> Result<usize, LockedList<'_, T, usize, SP>> {
        if let Ok(s) = self.try_update_state(set_order, fetch_order, |s| Some(f(s))) {
            return Ok(s);
        }
        let locked = self.lock();
        self.try_update_state(set_order, fetch_order, |s| Some(f(s)))
            .or(Err(locked))
    }

    #[inline]
    pub fn update_state_or_lock_with<
        'a,
        F: FnMut(usize) -> usize,
        L: FnOnce(LockedList<'a, T, usize, SP>),
    >(
        &'a self,
        set_order: Ordering,
        fetch_order: Ordering,
        mut f: F,
        locked_fallback: L,
    ) {
        if (self.try_update_state(set_order, fetch_order, |s| Some(f(s)))).is_err() {
            self.update_state_or_lock_with_cold(set_order, fetch_order, f, locked_fallback);
        }
    }

    #[cold]
    #[inline(never)]
    fn update_state_or_lock_with_cold<
        'a,
        F: FnMut(usize) -> usize,
        L: FnOnce(LockedList<'a, T, usize, SP>),
    >(
        &'a self,
        set_order: Ordering,
        fetch_order: Ordering,
        mut f: F,
        locked_fallback: L,
    ) {
        let locked = self.lock();
        if (self.try_update_state(set_order, fetch_order, |s| Some(f(s)))).is_err() {
            locked_fallback(locked);
        }
    }
}

impl<T, S: ListState, SP: SyncPrimitives> Default for List<T, S, SP> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct LockedList<'a, T, S: ListState = (), SP: SyncPrimitives = DefaultSyncPrimitives> {
    queue: &'a List<T, S, SP>,
    guard: ManuallyDrop<MutexGuard<'a, SP>>,
    _not_send: PhantomData<*mut ()>,
}

unsafe impl<'a, T, S: ListState, SP: SyncPrimitives> Sync for LockedList<'a, T, S, SP> {}

impl<'a, T, S: ListState, SP: SyncPrimitives> LockedList<'a, T, S, SP> {
    #[inline(always)]
    fn get_next(&self, next: &AtomicPtr<NodeLink>) -> NonNull<NodeLink> {
        if let Some(next) = NonNull::new(next.load(Acquire)) {
            return next;
        }
        self.wait_for_next(next)
    }

    #[cold]
    #[inline(never)]
    fn wait_for_next(&self, next: &AtomicPtr<NodeLink>) -> NonNull<NodeLink> {
        if SP::Parker::NEVER_BLOCKS {
            return unsafe { self.parker.park_until(|| NonNull::new(next.load(Acquire))) };
        }
        for _ in 0..SP::SPIN_BEFORE_PARK {
            hint::spin_loop();
            if let Some(next) = NonNull::new(next.load(Acquire)) {
                return next;
            }
        }
        const PARKED: *mut NodeLink = ptr::without_provenance_mut(1);
        if let Err(next) = next.compare_exchange(ptr::null_mut(), PARKED, Relaxed, Acquire) {
            return unsafe { NonNull::new_unchecked(next) };
        }
        let load_next = || {
            let next = next.load(Acquire);
            (next != PARKED).then(|| unsafe { NonNull::new_unchecked(next) })
        };
        unsafe { self.parker.park_until(load_next) }
    }

    #[inline]
    pub fn front(&mut self) -> Option<ListFront<'a, '_, T, S, SP>> {
        self.tail()?;
        let node = self.get_next(&self.queue.head);
        Some(ListFront { node, locked: self })
    }

    #[inline]
    pub fn back(&mut self) -> Option<ListBack<'a, '_, T, S, SP>> {
        let node = self.tail()?;
        Some(ListBack { node, locked: self })
    }

    pub fn unlock(self) -> &'a List<T, S, SP> {
        self.queue
    }

    #[inline(always)]
    pub(crate) unsafe fn remove<F: FnOnce() -> S>(
        &mut self,
        node: NonNull<NodeLink>,
        new_state_if_last_node: F,
        is_front: bool,
        is_back: bool,
    ) -> (Option<NonNull<NodeLink>>, Option<NonNull<NodeLink>>) {
        debug_assert!(!(is_front && is_back));
        let node = unsafe { node.as_ref() };
        let prev = if is_front {
            NonNull::new(HEAD_MARKER).unwrap()
        } else {
            // TODO safety the node is linked
            unsafe { NonNull::new_unchecked(node.prev.load(Relaxed)) }
        };
        let is_head = prev.as_ptr() == HEAD_MARKER;
        let prev_next = if is_head {
            &self.head
        } else {
            unsafe { &prev.as_ref().next }
        };
        if is_back {
            let prev_next = self.get_next(prev_next);
            debug_assert_eq!(prev_next, node.into());
        }
        let mut next = if is_back { None } else { node.next() };
        let mut tail = None;
        if next.is_none() {
            prev_next.store(ptr::null_mut(), Relaxed);
            let new_tail = if is_head {
                new_state_if_last_node().into_tail()
            } else {
                prev.into_tail()
            };
            let node_ptr = NonNull::from(node).into_tail();
            if let Err(t) = (self.tail).compare_exchange(node_ptr, new_tail, Release, Relaxed) {
                if is_back {
                    fence(Acquire);
                }
                tail = Some(unsafe { t.ptr().unwrap_unchecked() });
                next = Some(self.get_next(&node.next));
            } else if !is_head {
                tail = Some(prev);
            }
        }
        if let Some(next) = next {
            unsafe { next.as_ref().prev.store(prev.as_ptr(), Relaxed) };
            prev_next.store(next.as_ptr(), Relaxed);
        }
        node.prev.store(NULL, Release);
        (next, tail)
    }
}

impl<'a, T, SP: SyncPrimitives> LockedList<'a, T, (), SP> {
    #[inline]
    pub fn drain(self) -> Drain<'a, T, (), SP> {
        Drain::new(self, || ())
    }
}

impl<'a, T, SP: SyncPrimitives> LockedList<'a, T, usize, SP> {
    pub fn drain<F: FnOnce() -> usize>(self, new_state_if_not_empty: F) -> Drain<'a, T, usize, SP> {
        Drain::new(self, new_state_if_not_empty)
    }
}

impl<T, S: ListState, SP: SyncPrimitives> Drop for LockedList<'_, T, S, SP> {
    #[inline]
    fn drop(&mut self) {
        unsafe { self.queue.mutex.unlock(ManuallyDrop::take(&mut self.guard)) };
    }
}

impl<T, S: ListState, SP: SyncPrimitives> Deref for LockedList<'_, T, S, SP> {
    type Target = List<T, S, SP>;

    fn deref(&self) -> &Self::Target {
        self.queue
    }
}

pub trait ListEnd<'locked, 'a, T, S: ListState = (), SP: SyncPrimitives = DefaultSyncPrimitives>:
    NodeRef<T> + Sized
{
    fn unlink<F: FnOnce() -> S>(self, new_state_if_last_node: F) -> Option<Self>;
}

pub struct ListFront<'locked, 'a, T, S: ListState = (), SP: SyncPrimitives = DefaultSyncPrimitives>
{
    node: NonNull<NodeLink>,
    locked: &'a mut LockedList<'locked, T, S, SP>,
}

unsafe impl<'locked, T: Send, S: ListState, SP: SyncPrimitives> Send
    for ListFront<'locked, '_, T, S, SP>
where
    LockedList<'locked, T, S, SP>: Send,
{
}
unsafe impl<'locked, T: Sync, S: ListState, SP: SyncPrimitives> Sync
    for ListFront<'locked, '_, T, S, SP>
where
    LockedList<'locked, T, S, SP>: Sync,
{
}

impl<'locked, 'a, T, S: ListState, SP: SyncPrimitives> ListEnd<'locked, 'a, T, S, SP>
    for ListFront<'locked, 'a, T, S, SP>
{
    #[inline]
    fn unlink<F: FnOnce() -> S>(self, new_state_if_last_node: F) -> Option<Self> {
        let (next, _) =
            unsafe { (self.locked).remove(self.node, new_state_if_last_node, true, false) };
        Some(Self {
            node: next?,
            locked: self.locked,
        })
    }
}

impl<T, SP: SyncPrimitives> ListFront<'_, '_, T, (), SP> {
    pub fn unlink(self) -> Option<Self> {
        ListEnd::unlink(self, || ())
    }
}

impl<T, SP: SyncPrimitives> ListFront<'_, '_, T, usize, SP> {
    pub fn unlink<F: FnOnce() -> usize>(self, new_state_if_last_node: F) -> Option<Self> {
        ListEnd::unlink(self, new_state_if_last_node)
    }
}

node_ref!(
    ListFront<'locked, 'a, T, S: ListState, SP: SyncPrimitives>,
    T,
    self.node
);

pub struct ListBack<'locked, 'a, T, S: ListState = (), SP: SyncPrimitives = DefaultSyncPrimitives> {
    node: NonNull<NodeLink>,
    locked: &'a mut LockedList<'locked, T, S, SP>,
}

unsafe impl<'locked, T: Send, S: ListState, SP: SyncPrimitives> Send
    for ListBack<'locked, '_, T, S, SP>
where
    LockedList<'locked, T, S, SP>: Send,
{
}
unsafe impl<'locked, T: Sync, S: ListState, SP: SyncPrimitives> Sync
    for ListBack<'locked, '_, T, S, SP>
where
    LockedList<'locked, T, S, SP>: Sync,
{
}

impl<'locked, 'a, T, S: ListState, SP: SyncPrimitives> ListEnd<'locked, 'a, T, S, SP>
    for ListBack<'locked, 'a, T, S, SP>
{
    #[inline]
    fn unlink<F: FnOnce() -> S>(self, new_state_if_last_node: F) -> Option<Self> {
        let (_, tail) =
            unsafe { (self.locked).remove(self.node, new_state_if_last_node, false, true) };
        Some(Self {
            node: tail?,
            locked: self.locked,
        })
    }
}

impl<T, SP: SyncPrimitives> ListBack<'_, '_, T, (), SP> {
    pub fn unlink(self) -> Option<Self> {
        ListEnd::unlink(self, || ())
    }
}

impl<T, SP: SyncPrimitives> ListBack<'_, '_, T, usize, SP> {
    pub fn unlink<F: FnOnce() -> usize>(self, new_state_if_last_node: F) -> Option<Self> {
        ListEnd::unlink(self, new_state_if_last_node)
    }
}

node_ref!(
    ListBack<'locked, 'a, T, S: ListState, SP: SyncPrimitives>,
    T,
    self.node
);

pub struct GetFront;
pub struct GetBack;

pub trait ListGetEnd {
    type ListEnd<'locked, 'a, T, S: ListState, SP: SyncPrimitives>: ListEnd<'locked, 'a, T, S, SP>
    where
        'locked: 'a,
        T: 'locked;

    fn get_end<'locked, 'a, T, S: ListState, SP: SyncPrimitives>(
        locked: &'a mut LockedList<'locked, T, S, SP>,
    ) -> Option<Self::ListEnd<'locked, 'a, T, S, SP>>;
}

impl ListGetEnd for GetFront {
    type ListEnd<'locked, 'a, T, S: ListState, SP: SyncPrimitives>
        = ListFront<'locked, 'a, T, S, SP>
    where
        'locked: 'a,
        T: 'locked;

    #[inline]
    fn get_end<'locked, 'a, T, S: ListState, SP: SyncPrimitives>(
        locked: &'a mut LockedList<'locked, T, S, SP>,
    ) -> Option<Self::ListEnd<'locked, 'a, T, S, SP>> {
        locked.front()
    }
}

impl ListGetEnd for GetBack {
    type ListEnd<'locked, 'a, T, S: ListState, SP: SyncPrimitives>
        = ListBack<'locked, 'a, T, S, SP>
    where
        'locked: 'a,
        T: 'locked;

    #[inline]
    fn get_end<'locked, 'a, T, S: ListState, SP: SyncPrimitives>(
        locked: &'a mut LockedList<'locked, T, S, SP>,
    ) -> Option<Self::ListEnd<'locked, 'a, T, S, SP>> {
        locked.back()
    }
}

/// # Safety
///
/// For a given instance, [`Self::as_list`] must always return a reference to the same list.
pub unsafe trait AsList<L> {
    fn as_list(&self) -> &L;
}

unsafe impl<L, R: AsList<L>> AsList<L> for &R {
    fn as_list(&self) -> &L {
        (**self).as_list()
    }
}

#[cfg(feature = "alloc")]
unsafe impl<L, R: AsList<L>> AsList<L> for alloc::sync::Arc<R> {
    fn as_list(&self) -> &L {
        (**self).as_list()
    }
}

#[macro_export]
macro_rules! as_list {
    ($ty:ident$(<$($lf:lifetime)? $(,)? $($arg:ident $(: $bound:path)?),* $(,)?>)?, $list:ty, &self $(.$field:tt)+ $(,)?) => {
        unsafe impl $(<
            $($lf,)?
            $($arg $(: $bound)?,)*
        >)? $crate::list::AsList<$list> for $ty $(<$($lf,)? $($arg,)*>)? {
            fn as_list(&self) -> &$list {
                &self $(.$field)+
            }
        }
    };
}
