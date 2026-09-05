#[cfg(feature = "alloc")]
extern crate alloc;

use core::{marker::PhantomData, mem::ManuallyDrop, ops::Deref, ptr, ptr::NonNull};

use crate::{
    loom::{
        AtomicPtrExt,
        sync::atomic::{AtomicPtr, Ordering, Ordering::*, fence},
    },
    node::{NodeLink, node_ref},
    sync::mutex::{DefaultMutex, Mutex},
};

mod drain;
mod linking;
pub(crate) mod state;

pub use drain::*;
pub use linking::*;
pub use state::*;

use crate::node::NodeRef;

type MutexGuard<'a, M> = <M as Mutex>::Guard<'a>;

const HEAD_MARKER: usize = 1;

pub struct List<T, S: ListState = (), L: Linking = Eager, M: Mutex = DefaultMutex> {
    tail: AtomicPtr<Tail<S, L>>,
    head: L::NextPtr,
    mutex: M,
    parker: L::Parker,
    _node_data: PhantomData<T>,
}

unsafe impl<T: Send, S: ListState, L: Linking, M: Mutex> Send for List<T, S, L, M> {}
unsafe impl<T: Send, S: ListState, L: Linking, M: Mutex> Sync for List<T, S, L, M> {}

impl<T, S: ListState, L: Linking, M: Mutex> List<T, S, L, M> {
    #[cfg_attr(loom, const_fn::const_fn(cfg(false)))]
    const fn new_impl(tail: *mut Tail<S, L>) -> Self {
        Self {
            tail: AtomicPtr::new(tail),
            #[cfg(not(loom))]
            head: L::NEW_NEXT,
            #[cfg(loom)]
            head: L::new_next(None),
            #[cfg(not(loom))]
            mutex: M::INIT,
            #[cfg(loom)]
            mutex: M::new(),
            #[cfg(not(loom))]
            parker: L::NEW_PARKER,
            #[cfg(loom)]
            parker: L::new_parker(),
            _node_data: PhantomData,
        }
    }

    #[cfg_attr(loom, const_fn::const_fn(cfg(false)))]
    #[inline]
    pub const fn new() -> Self {
        Self::new_impl(ptr::null_mut())
    }

    #[inline(always)]
    fn tail(&self) -> Option<NonNull<NodeLink<L>>> {
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
    pub fn lock(&self) -> LockedList<'_, T, S, L, M> {
        LockedList {
            queue: self,
            guard: ManuallyDrop::new(self.mutex.lock()),
            _not_send: PhantomData,
        }
    }

    pub(crate) unsafe fn push_back(
        &self,
        mut node: NonNull<NodeLink<L>>,
        set_order: Ordering,
        fetch_order: Ordering,
        mut f: Option<impl FnMut(S) -> Option<S>>,
        mut on_push: impl FnMut(Option<S>) -> bool,
        on_pushed: impl FnOnce(),
    ) -> Result<S, bool> {
        let set_order = L::push_back_set_order(set_order);
        let mut tail = self.tail.load(fetch_order);
        let prev = loop {
            let (new_tail, prev) = match S::tail_to_enum(tail) {
                StateOrPtr::State(state)
                    if let Some(f) = f.as_mut()
                        && let Some(new_state) = f(state) =>
                {
                    (new_state.into_tail(), ptr::null_mut())
                }
                state_or_ptr if !on_push(state_or_ptr.state()) => {
                    unsafe { node.as_mut().prev.store_mut(ptr::null_mut()) };
                    return Err(false);
                }
                StateOrPtr::State(_) => {
                    (node.into_tail(), ptr::without_provenance_mut(HEAD_MARKER))
                }
                StateOrPtr::Ptr(prev) => (node.into_tail(), prev.as_ptr()),
            };
            unsafe { node.as_mut().prev.store_mut(prev) };
            match (self.tail).compare_exchange_weak(tail, new_tail, set_order, fetch_order) {
                Ok(_) => break prev,
                Err(t) => tail = t,
            }
        };
        let prev_next = match prev.addr() {
            0 if f.is_some() => return Ok(unsafe { tail.state().unwrap_unchecked() }),
            HEAD_MARKER => NonNull::from(&self.head),
            _ => unsafe { NonNull::new_unchecked((&raw const (*prev).next).cast_mut()) },
        };
        // TODO must be called before unpark in case unpark panics
        on_pushed();
        L::store_next(prev_next, node, &self.parker);
        Err(true)
    }
}

impl<T, L: Linking, M: Mutex> List<T, usize, L, M> {
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
    ) -> Result<usize, LockedList<'_, T, usize, L, M>> {
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
        G: FnOnce(LockedList<'a, T, usize, L, M>),
    >(
        &'a self,
        set_order: Ordering,
        fetch_order: Ordering,
        mut f: F,
        locked_fallback: G,
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
        G: FnOnce(LockedList<'a, T, usize, L, M>),
    >(
        &'a self,
        set_order: Ordering,
        fetch_order: Ordering,
        mut f: F,
        locked_fallback: G,
    ) {
        let locked = self.lock();
        if (self.try_update_state(set_order, fetch_order, |s| Some(f(s)))).is_err() {
            locked_fallback(locked);
        }
    }
}

impl<T, S: ListState, L: Linking, M: Mutex> Default for List<T, S, L, M> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct LockedList<'a, T, S: ListState = (), L: Linking = Eager, M: Mutex = DefaultMutex> {
    queue: &'a List<T, S, L, M>,
    guard: ManuallyDrop<MutexGuard<'a, M>>,
    _not_send: PhantomData<*mut ()>,
}

unsafe impl<'a, T: Send, S: ListState, L: Linking, M: Mutex> Sync for LockedList<'a, T, S, L, M> {}

impl<'a, T, S: ListState, L: Linking, M: Mutex> LockedList<'a, T, S, L, M> {
    /// [`Linking::get_next`] with the list's head slot and parker filled in.
    #[inline(always)]
    fn get_next(
        &self,
        node: Option<NonNull<NodeLink<L>>>,
        next: &L::NextPtr,
        tail: NonNull<NodeLink<L>>,
    ) -> NonNull<NodeLink<L>> {
        L::get_next(node, next, tail, &self.head, &self.parker)
    }

    #[inline]
    pub fn front(&mut self) -> Option<ListFront<'a, '_, T, S, L, M>>
    where
        L: Linking,
    {
        let node = self.get_next(None, &self.queue.head, self.tail()?);
        Some(ListFront { node, locked: self })
    }

    #[inline]
    pub fn back(&mut self) -> Option<ListBack<'a, '_, T, S, L, M>> {
        let node = self.tail()?;
        Some(ListBack { node, locked: self })
    }

    pub fn unlock(self) -> &'a List<T, S, L, M> {
        self.queue
    }

    #[allow(clippy::type_complexity)]
    #[inline(always)]
    pub(crate) unsafe fn remove<F: FnOnce() -> S>(
        &mut self,
        node: NonNull<NodeLink<L>>,
        new_state_if_last_node: F,
        is_front: bool,
        is_back: bool,
    ) -> (Option<NonNull<NodeLink<L>>>, Option<NonNull<NodeLink<L>>>) {
        debug_assert!(!(is_front && is_back));
        // TODO for self-removal with LazyDoubly/Singly, the tail may not have been acquired
        // (it was written with at least Release in push_back, but a fence(Acquire) would not
        // work as the task may have moved in another thread)
        if L::PREV_OR_GET_NEXT_REQUIRES_TAIL_ACQUIRE && !is_front && !is_back {
            self.tail();
        }
        let node = unsafe { node.as_ref() };
        let prev = if is_front {
            NonNull::new(ptr::without_provenance_mut(HEAD_MARKER)).unwrap()
        } else {
            // TODO safety the node is linked
            unsafe { NonNull::new_unchecked(node.prev.load(Relaxed)) }
        };
        let is_head = prev.addr().get() == HEAD_MARKER;
        let prev_next = if is_head {
            &self.head
        } else {
            unsafe { &prev.as_ref().next }
        };
        if is_back {
            L::wait_next(prev_next, &self.parker);
        }
        let mut next = if is_back {
            None
        } else {
            L::load_next(&node.next)
        };
        let mut tail = None;
        if next.is_none() {
            L::update_next(prev_next, None);
            let new_tail = if is_head {
                new_state_if_last_node().into_tail()
            } else {
                prev.into_tail()
            };
            let node_ptr = NonNull::from(node).into_tail();
            if let Err(t) = (self.tail).compare_exchange(node_ptr, new_tail, Release, Relaxed) {
                if is_back || L::PREV_OR_GET_NEXT_REQUIRES_TAIL_ACQUIRE {
                    fence(Acquire);
                }
                tail = Some(unsafe { t.ptr().unwrap_unchecked() });
                next = Some(self.get_next(Some(node.into()), &node.next, tail.unwrap()));
            } else if !is_head {
                tail = Some(prev);
            }
        }
        if let Some(next) = next {
            unsafe { next.as_ref().prev.store(prev.as_ptr(), Relaxed) };
            L::update_next(prev_next, Some(next));
        }
        L::update_next(&node.next, None);
        node.prev.store(ptr::null_mut(), Release);
        (next, tail)
    }
}

impl<'a, T, L: Linking, M: Mutex> LockedList<'a, T, (), L, M> {
    #[inline]
    pub fn drain(self) -> Drain<'a, T, (), L, M> {
        Drain::new(self, || ())
    }
}

impl<'a, T, L: Linking, M: Mutex> LockedList<'a, T, usize, L, M> {
    pub fn drain<F: FnOnce() -> usize>(
        self,
        new_state_if_not_empty: F,
    ) -> Drain<'a, T, usize, L, M> {
        Drain::new(self, new_state_if_not_empty)
    }
}

impl<T, S: ListState, L: Linking, M: Mutex> Drop for LockedList<'_, T, S, L, M> {
    #[inline]
    fn drop(&mut self) {
        unsafe { self.queue.mutex.unlock(ManuallyDrop::take(&mut self.guard)) };
    }
}

impl<T, S: ListState, L: Linking, M: Mutex> Deref for LockedList<'_, T, S, L, M> {
    type Target = List<T, S, L, M>;

    fn deref(&self) -> &Self::Target {
        self.queue
    }
}

pub trait ListEnd<'locked, 'a, T, S: ListState = (), L: Linking = Eager, M: Mutex = DefaultMutex>:
    NodeRef<T> + Sized
{
    fn unlink<F: FnOnce() -> S>(self, new_state_if_last_node: F) -> Option<Self>;
}

pub struct ListFront<'locked, 'a, T, S: ListState = (), L: Linking = Eager, M: Mutex = DefaultMutex>
{
    node: NonNull<NodeLink<L>>,
    locked: &'a mut LockedList<'locked, T, S, L, M>,
}

unsafe impl<'locked, T: Send, S: ListState, L: Linking, M: Mutex> Send
    for ListFront<'locked, '_, T, S, L, M>
where
    LockedList<'locked, T, S, L, M>: Send,
{
}
unsafe impl<'locked, T: Sync, S: ListState, L: Linking, M: Mutex> Sync
    for ListFront<'locked, '_, T, S, L, M>
where
    LockedList<'locked, T, S, L, M>: Sync,
{
}

impl<'locked, 'a, T, S: ListState, L: Linking, M: Mutex> ListEnd<'locked, 'a, T, S, L, M>
    for ListFront<'locked, 'a, T, S, L, M>
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

impl<T, L: Linking, M: Mutex> ListFront<'_, '_, T, (), L, M> {
    pub fn unlink(self) -> Option<Self> {
        ListEnd::unlink(self, || ())
    }
}

impl<T, L: Linking, M: Mutex> ListFront<'_, '_, T, usize, L, M> {
    pub fn unlink<F: FnOnce() -> usize>(self, new_state_if_last_node: F) -> Option<Self> {
        ListEnd::unlink(self, new_state_if_last_node)
    }
}

node_ref!(
    ListFront<'locked, 'a, T, S: ListState, L: Linking, M: Mutex>,
    T,
    L,
    self.node
);

pub struct ListBack<'locked, 'a, T, S: ListState = (), L: Linking = Eager, M: Mutex = DefaultMutex>
{
    node: NonNull<NodeLink<L>>,
    locked: &'a mut LockedList<'locked, T, S, L, M>,
}

unsafe impl<'locked, T: Send, S: ListState, L: Linking, M: Mutex> Send
    for ListBack<'locked, '_, T, S, L, M>
where
    LockedList<'locked, T, S, L, M>: Send,
{
}
unsafe impl<'locked, T: Sync, S: ListState, L: Linking, M: Mutex> Sync
    for ListBack<'locked, '_, T, S, L, M>
where
    LockedList<'locked, T, S, L, M>: Sync,
{
}

impl<'locked, 'a, T, S: ListState, L: Linking, M: Mutex> ListEnd<'locked, 'a, T, S, L, M>
    for ListBack<'locked, 'a, T, S, L, M>
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

impl<T, L: Linking, M: Mutex> ListBack<'_, '_, T, (), L, M> {
    pub fn unlink(self) -> Option<Self> {
        ListEnd::unlink(self, || ())
    }
}

impl<T, L: Linking, M: Mutex> ListBack<'_, '_, T, usize, L, M> {
    pub fn unlink<F: FnOnce() -> usize>(self, new_state_if_last_node: F) -> Option<Self> {
        ListEnd::unlink(self, new_state_if_last_node)
    }
}

node_ref!(
    ListBack<'locked, 'a, T, S: ListState, L: Linking, M: Mutex>,
    T,
    L,
    self.node
);

pub struct GetFront;
pub struct GetBack;

pub trait ListGetEnd<L: Linking> {
    type ListEnd<'locked, 'a, T, S: ListState, M: Mutex>: ListEnd<'locked, 'a, T, S, L, M>
    where
        'locked: 'a,
        T: 'locked;

    fn get_end<'locked, 'a, T, S: ListState, M: Mutex>(
        locked: &'a mut LockedList<'locked, T, S, L, M>,
    ) -> Option<Self::ListEnd<'locked, 'a, T, S, M>>;
}

impl<L: Linking> ListGetEnd<L> for GetFront {
    type ListEnd<'locked, 'a, T, S: ListState, M: Mutex>
        = ListFront<'locked, 'a, T, S, L, M>
    where
        'locked: 'a,
        T: 'locked;

    #[inline]
    fn get_end<'locked, 'a, T, S: ListState, M: Mutex>(
        locked: &'a mut LockedList<'locked, T, S, L, M>,
    ) -> Option<Self::ListEnd<'locked, 'a, T, S, M>> {
        locked.front()
    }
}

impl<L: Linking> ListGetEnd<L> for GetBack {
    type ListEnd<'locked, 'a, T, S: ListState, M: Mutex>
        = ListBack<'locked, 'a, T, S, L, M>
    where
        'locked: 'a,
        T: 'locked;

    #[inline]
    fn get_end<'locked, 'a, T, S: ListState, M: Mutex>(
        locked: &'a mut LockedList<'locked, T, S, L, M>,
    ) -> Option<Self::ListEnd<'locked, 'a, T, S, M>> {
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
