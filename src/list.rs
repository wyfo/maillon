//! The [`List`] and the types to operate on it.
use core::{cell::UnsafeCell, marker::PhantomData, mem::ManuallyDrop, pin::Pin, ptr::NonNull};

#[allow(unused_imports)]
use crate::msrv::StrictProvenance;
use crate::{
    backoff::BackoffState,
    loom::{
        AtomicPtrExt,
        sync::atomic::{AtomicPtr, Ordering, Ordering::*, fence},
    },
    msrv::ptr,
    node::{NodeData, NodeLink, NodeRef, NodeUnlinked, PrivateNodeRef, node_ref},
    sync::mutex::{DefaultMutex, Mutex},
    utils::abort_on_unwind,
};

mod cursor;
mod drain;
pub(crate) mod state;

pub use cursor::*;
pub use drain::*;
pub use state::*;

use crate::linking::{AtomicEager, Linking, PrivateLinking, Serialized};

type MutexGuard<'a, M> = <M as Mutex>::Guard<'a>;

pub(crate) const HEAD_MARKER: usize = 1;

/// A concurrent intrusive list.
///
/// It contains [`Node`](crate::Node)s carrying the data `T`; they are pushed at the back of the
/// list through [`NodeUnlinked`], and can then be removed from any position with the list
/// [`locked`](Self::lock) on its mutex `M`.
///
/// # List state
///
/// While the list is empty, it embeds a [`ListState`] `S`. The state is especially used as an
/// emptiness marker, loaded atomically with [`is_empty`](Self::is_empty).
///
/// With `S=usize`, multiple methods allow state manipulation, and it can atomically condition the
/// push of a node with [`NodeUnlinked::try_update_state_or_push_back_with`].
///
/// # List data
///
/// The list carries mutex-protected data `D` that can be accessed with the list locked. Because
/// Rust doesn't allow partial borrowing while linked node access requires a mutable borrow on the
/// [`LockedList`] guard, the linked nodes' accessors also allow accessing the list data through
/// [`LinkedNodeRef`](crate::node::LinkedNodeRef).
///
/// # Linking
///
/// On their insertion, nodes are linked to the others according to the [`Linking`] `L` parameter.
/// Linking strategy especially determines how the list state is mutated.
///
/// With the default [`AtomicEager`], the list state mutations are lock-free, and node push to the
/// back of the list is also lock-free on mainstream platforms.
///
/// Regardless of the linking, state access in
/// [`is_empty`](Self::is_empty)/[`load_state`](Self::load_state) is always a single atomic load.
///
/// # Mutex
///
/// The [`Mutex`] `M` is mainly used to serialize node removal (and insertion with [`Serialized`]
/// linking). However, its reentrancy is not specified, so arbitrary code, e.g.
/// [`Waker::wake`](core::task::Waker::wake), should not be executed while the list is locked.
///
/// As a node must be unlinked on drop, and thus must acquire the mutex, locking cannot fail:
/// unwinding in `Mutex::lock` causes the process to abort.
pub struct List<T, S: ListState = (), D = (), L: Linking = AtomicEager, M: Mutex = DefaultMutex> {
    tail: AtomicPtr<Tail<S, L>>,
    head: L::NextPtr,
    mutex: M,
    parker: L::Parker,
    data: UnsafeCell<D>,
    #[cfg(loom)]
    data_access: crate::loom::cell::Cell<()>, // same trick as `NodeLink::data_ptr`
    _node_data: PhantomData<T>,
}

unsafe impl<T: Send, S: ListState, D: Send, L: Linking, M: Mutex> Send for List<T, S, D, L, M> {}
unsafe impl<T: Send, S: ListState, D: Send, L: Linking, M: Mutex> Sync for List<T, S, D, L, M> {}

impl<T, S: ListState, L: Linking, M: Mutex> List<T, S, (), L, M> {
    /// Creates an empty list.
    #[cfg_attr(loom, const_fn::const_fn(cfg(false)))]
    #[inline]
    pub const fn new() -> Self {
        Self::new_impl(ptr::null_mut(), ())
    }
}

impl<T, S: ListState, D, L: Linking, M: Mutex> List<T, S, D, L, M> {
    #[cfg_attr(loom, const_fn::const_fn(cfg(false)))]
    const fn new_impl(tail: *mut Tail<S, L>, data: D) -> Self {
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
            data: UnsafeCell::new(data),
            #[cfg(loom)]
            data_access: crate::loom::cell::Cell::new(()),
            _node_data: PhantomData,
        }
    }

    /// Creates an empty list with the given list data.
    #[cfg_attr(loom, const_fn::const_fn(cfg(false)))]
    #[inline]
    pub const fn with_data(data: D) -> Self {
        Self::new_impl(ptr::null_mut(), data)
    }

    /// Returns a mutable reference to the list data.
    #[inline]
    pub fn data_mut(&mut self) -> &mut D {
        self.data.get_mut()
    }

    /// Consumes the list and returns its data.
    #[inline]
    pub fn into_data(self) -> D {
        self.data.into_inner()
    }

    #[inline(always)]
    fn tail(&self) -> Option<NonNull<NodeLink<L>>> {
        let order = if L::SERIALIZED { Relaxed } else { Acquire };
        self.tail.load(order).ptr()
    }

    #[inline(always)]
    fn store_tail_serialized(&self, new_tail: *mut Tail<S, L>, order: Ordering, is_empty: bool) {
        debug_assert!(L::SERIALIZED && is_empty == self.tail().is_none());
        if !is_empty {
            self.tail.store(new_tail, Relaxed);
        } else if matches!(order, Acquire | AcqRel) {
            self.tail.swap(new_tail, order);
        } else {
            self.tail.store(new_tail, order);
        }
    }

    /// Loads the list state with the ordering `order`, and returns `true` if the list is empty.
    #[inline]
    pub fn is_empty(&self, order: Ordering) -> bool {
        self.tail.load(order).ptr().is_none()
    }

    /// Acquires the list's mutex.
    #[inline]
    pub fn lock(&self) -> LockedList<'_, T, S, D, L, M> {
        LockedList {
            list: self,
            guard: ManuallyDrop::new(abort_on_unwind(|| self.mutex.lock())),
            _not_send: PhantomData,
        }
    }

    #[allow(clippy::incompatible_msrv, unstable_name_collisions)]
    pub(crate) fn push_back<LR>(
        &self,
        mut node: NodeUnlinked<'_, LR>,
        set_order: Ordering,
        fetch_order: Ordering,
        mut f: Option<impl FnMut(Pin<&mut T>, S) -> Option<S>>,
        mut on_push: impl FnMut(Pin<&mut T>, Option<S>) -> bool,
    ) -> Result<S, bool>
    where
        LR: ListRef<NodeData = T, ListState = S, ListData = D, Linking = L, Mutex = M>,
    {
        debug_assert!(ptr::eq(node.list().as_list(), self));
        let mut link = node.link();
        let _locked = L::SERIALIZED.then(|| self.lock());
        let set_order = L::push_back_set_order(set_order);
        let mut backoff = BackoffState::new(L::Backoff::default());
        let mut tail = self.tail.load(fetch_order);
        let prev = loop {
            let state_or_ptr = S::tail_to_enum(tail);
            let new_state =
                (state_or_ptr.state()).and_then(|state| f.as_mut()?(node.data_mut(), state));
            let (new_tail, prev) = match new_state {
                Some(new_state) => (new_state.into_tail(), ptr::null_mut()),
                None if !on_push(node.data_mut(), state_or_ptr.state()) => {
                    unsafe { link.as_mut().prev.store_mut(ptr::null_mut()) };
                    return Err(false);
                }
                None => match state_or_ptr {
                    StateOrPtr::State(_) => {
                        (link.into_tail(), ptr::without_provenance_mut(HEAD_MARKER))
                    }
                    StateOrPtr::Ptr(prev) => (link.into_tail(), prev.as_ptr()),
                },
            };
            unsafe { link.as_mut().prev.store_mut(prev) };
            if L::SERIALIZED {
                self.store_tail_serialized(new_tail, set_order, state_or_ptr.state().is_some());
                break prev;
            }
            if backoff.backoff_reload(&mut tail, || self.tail.load(fetch_order)) {
                continue;
            }
            match (self.tail).compare_exchange_weak(tail, new_tail, set_order, fetch_order) {
                Ok(_) => break prev,
                Err(t) => tail = t,
            }
        };
        if f.is_some() && prev.is_null() {
            return Ok(unsafe { tail.state().unwrap_unchecked() });
        }
        // `addr_of!((*prev).next)` can't be used with AtomicLazy as the previous node might have
        // been concurrently dequeued.
        L::store_next(prev, &self.head, link, &self.parker);
        node.set_linked(self);
        Err(true)
    }
}

impl<T, L: Linking, M: Mutex> List<T, usize, (), L, M> {
    /// Creates an empty list with the given state.
    #[cfg_attr(loom, const_fn::const_fn(cfg(false)))]
    #[inline]
    pub const fn with_state(state: usize) -> Self {
        Self::new_impl(state_to_ptr(state), ())
    }
}

impl<T, D, L: Linking, M: Mutex> List<T, usize, D, L, M> {
    /// Creates an empty list with the given state and list data.
    #[cfg_attr(loom, const_fn::const_fn(cfg(false)))]
    #[inline]
    pub const fn with_state_and_data(state: usize, data: D) -> Self {
        Self::new_impl(state_to_ptr(state), data)
    }

    /// Loads the list state with the ordering `order`, and returns `None` if the list is not
    /// empty.
    #[inline]
    pub fn load_state(&self, order: Ordering) -> Option<usize> {
        self.tail.load(order).state()
    }

    /// Returns the list state, `None` if the list is not empty.
    #[inline]
    pub fn get_state(&mut self) -> Option<usize> {
        self.tail.load_mut().state()
    }

    /// Sets the list state if the list is empty, and returns whether it has been set.
    #[inline]
    pub fn try_set_state(&mut self, state: usize) -> bool {
        if self.get_state().is_none() {
            return false;
        }
        self.tail.store_mut(state.into_tail());
        true
    }

    /// Updates the list state from `current` to `new` if it matches, as
    /// [`AtomicUsize::compare_exchange`](core::sync::atomic::AtomicUsize::compare_exchange).
    ///
    /// Returns `Err(Some(state))` if the state doesn't match, and `Err(None)` if the list is not
    /// empty.
    #[inline]
    pub fn compare_exchange_state(
        &self,
        current: usize,
        new: usize,
        success: Ordering,
        failure: Ordering,
    ) -> Result<usize, Option<usize>> {
        if L::SERIALIZED {
            self.lock()
                .compare_exchange_state(current, new, success, failure)
        } else {
            self.compare_exchange_state_atomic(current, new, success, failure)
        }
    }

    #[inline]
    fn compare_exchange_state_atomic(
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

    /// Updates the list state with `f` if the list is empty, as
    /// [`AtomicUsize::fetch_update`](core::sync::atomic::AtomicUsize::fetch_update).
    ///
    /// Returns `Err(Some(state))` if `f` returns `None`, and `Err(None)` if the list is not
    /// empty.
    #[inline]
    pub fn try_update_state<F: FnMut(usize) -> Option<usize>>(
        &self,
        set_order: Ordering,
        fetch_order: Ordering,
        f: F,
    ) -> Result<usize, Option<usize>> {
        if L::SERIALIZED {
            self.lock().try_update_state(set_order, fetch_order, f)
        } else {
            self.try_update_state_atomic(set_order, fetch_order, f)
        }
    }

    #[inline]
    fn try_update_state_atomic<F: FnMut(usize) -> Option<usize>>(
        &self,
        set_order: Ordering,
        fetch_order: Ordering,
        mut f: F,
    ) -> Result<usize, Option<usize>> {
        let mut backoff = BackoffState::new(L::Backoff::default());
        let mut tail = self.tail.load(fetch_order);
        while let Some(state) = tail.state() {
            let Some(new_state) = f(state) else {
                return Err(Some(state));
            };
            let new_tail = new_state.into_tail();
            if backoff.backoff_reload(&mut tail, || self.tail.load(fetch_order)) {
                continue;
            }
            match (self.tail).compare_exchange_weak(tail, new_tail, set_order, fetch_order) {
                Ok(_) => return Ok(state),
                Err(ptr) => tail = ptr,
            }
        }
        Err(None)
    }

    /// Updates the list state with `f` as [`try_update_state`](Self::try_update_state), or
    /// returns the list locked if it is not empty.
    #[inline]
    pub fn update_state_or_lock<F: FnMut(usize) -> usize>(
        &self,
        set_order: Ordering,
        fetch_order: Ordering,
        mut f: F,
    ) -> Result<usize, LockedList<'_, T, usize, D, L, M>> {
        if !L::SERIALIZED {
            if let Ok(s) = self.try_update_state_atomic(set_order, fetch_order, |s| Some(f(s))) {
                return Ok(s);
            }
        }
        let mut locked = self.lock();
        locked
            .try_update_state(set_order, fetch_order, |s| Some(f(s)))
            .or(Err(locked))
    }

    /// Updates the list state with `f` as [`try_update_state`](Self::try_update_state), or calls
    /// `locked_fallback` with the list locked if it is not empty.
    ///
    /// Compared to [`update_state_or_lock`](Self::update_state_or_lock), the locked fallback is
    /// executed in a `#[cold]` function.
    #[inline]
    pub fn update_state_or_lock_with<
        'a,
        F: FnMut(usize) -> usize,
        G: FnOnce(LockedList<'a, T, usize, D, L, M>),
    >(
        &'a self,
        set_order: Ordering,
        fetch_order: Ordering,
        mut f: F,
        locked_fallback: G,
    ) {
        if L::SERIALIZED {
            let mut locked = self.lock();
            if (locked.try_update_state(set_order, fetch_order, |s| Some(f(s)))).is_err() {
                locked_fallback(locked);
            }
        } else if (self.try_update_state_atomic(set_order, fetch_order, |s| Some(f(s)))).is_err() {
            self.update_state_or_lock_with_cold(set_order, fetch_order, f, locked_fallback);
        }
    }

    #[cold]
    #[inline(never)]
    fn update_state_or_lock_with_cold<
        'a,
        F: FnMut(usize) -> usize,
        G: FnOnce(LockedList<'a, T, usize, D, L, M>),
    >(
        &'a self,
        set_order: Ordering,
        fetch_order: Ordering,
        mut f: F,
        locked_fallback: G,
    ) {
        let mut locked = self.lock();
        if (locked.try_update_state(set_order, fetch_order, |s| Some(f(s)))).is_err() {
            locked_fallback(locked);
        }
    }
}

impl<T, S: ListState, D: Default, L: Linking, M: Mutex> Default for List<T, S, D, L, M> {
    fn default() -> Self {
        Self::with_data(D::default())
    }
}

/// A reference to a [`List`], stored in each [`Node`](crate::Node) and passed to its
/// [`NodeData`] callbacks.
///
/// It is implemented by `List` and `&List`, and can be implemented by wrapper types, e.g. to give
/// the callbacks access to a structure embedding the list.
pub trait ListRef {
    /// The data carried by the nodes.
    type NodeData: NodeData<Self>;
    /// The state embedded in the list while it is empty.
    type ListState: ListState;
    /// The data carried by the list.
    type ListData;
    /// The linking implementation.
    type Linking: Linking;
    /// The mutex implementation.
    type Mutex: Mutex;

    /// Returns the referenced list.
    #[allow(clippy::type_complexity)]
    fn as_list(
        &self,
    ) -> &List<Self::NodeData, Self::ListState, Self::ListData, Self::Linking, Self::Mutex>;
}

impl<T: NodeData<Self>, S: ListState, D, L: Linking, M: Mutex> ListRef for List<T, S, D, L, M> {
    type NodeData = T;
    type ListState = S;
    type ListData = D;
    type Linking = L;
    type Mutex = M;

    fn as_list(&self) -> &Self {
        self
    }
}

impl<T: NodeData<Self>, S: ListState, D, L: Linking, M: Mutex> ListRef for &List<T, S, D, L, M> {
    type NodeData = T;
    type ListState = S;
    type ListData = D;
    type Linking = L;
    type Mutex = M;

    fn as_list(&self) -> &List<T, S, D, L, M> {
        self
    }
}

/// A locked [`List`], obtained from [`List::lock`].
///
/// Removing nodes and walking the list require the lock.
pub struct LockedList<
    'a,
    T,
    S: ListState = (),
    D = (),
    L: Linking = AtomicEager,
    M: Mutex = DefaultMutex,
> {
    list: &'a List<T, S, D, L, M>,
    guard: ManuallyDrop<MutexGuard<'a, M>>,
    _not_send: PhantomData<*mut ()>,
}

unsafe impl<'a, T: Send, S: ListState, D: Sync, L: Linking, M: Mutex> Sync
    for LockedList<'a, T, S, D, L, M>
{
}

impl<'a, T, S: ListState, D, L: Linking, M: Mutex> LockedList<'a, T, S, D, L, M> {
    /// See [`List::is_empty`].
    #[inline]
    pub fn is_empty(&self, order: Ordering) -> bool {
        self.list.is_empty(order)
    }

    /// [`Linking::get_next`] with the list's head slot and parker filled in.
    #[inline(always)]
    fn get_next(
        &self,
        node: Option<NonNull<NodeLink<L>>>,
        next: &L::NextPtr,
        tail: NonNull<NodeLink<L>>,
    ) -> NonNull<NodeLink<L>> {
        L::get_next(node, next, tail, &self.list.parker)
    }

    /// Returns the node at the end `E` of the list, `None` if it is empty.
    #[inline]
    pub fn end<E: End>(&mut self) -> Option<ListEnd<'a, '_, E, T, S, D, L, M>> {
        let node = if E::IS_FRONT {
            if L::SERIALIZED {
                L::load_next(&self.list.head)?
            } else {
                self.get_next(None, &self.list.head, self.list.tail()?)
            }
        } else {
            self.list.tail()?
        };
        Some(ListEnd {
            node,
            locked: self,
            _end: PhantomData,
        })
    }

    /// Returns the front node of the list, `None` if it is empty.
    #[inline]
    pub fn front(&mut self) -> Option<ListFront<'a, '_, T, S, D, L, M>> {
        self.end()
    }

    /// Returns the back node of the list, `None` if it is empty.
    #[inline]
    pub fn back(&mut self) -> Option<ListBack<'a, '_, T, S, D, L, M>> {
        self.end()
    }

    /// Returns a cursor pointing to the front node of the list, or to the ghost node if it is
    /// empty.
    #[inline]
    pub fn cursor_front(&mut self) -> ListCursor<'a, '_, T, S, D, L, M> {
        ListCursor::new(self.front().map(|f| f.node), self)
    }

    /// Returns a cursor pointing to the back node of the list, or to the ghost node if it is
    /// empty.
    #[inline]
    pub fn cursor_back(&mut self) -> ListCursor<'a, '_, T, S, D, L, M> {
        ListCursor::new(self.back().map(|t| t.node), self)
    }

    /// Unlocks the list and returns it.
    pub fn unlock(self) -> &'a List<T, S, D, L, M> {
        self.list
    }

    #[inline(always)]
    pub(crate) fn data_ptr(&self) -> *mut D {
        // same trick as `NodeLink::data_ptr` for loom
        #[cfg(loom)]
        self.list.data_access.set(());
        self.list.data.get()
    }

    /// Returns a reference to the list data.
    #[inline]
    pub fn data(&self) -> &D {
        unsafe { &*self.data_ptr() }
    }

    /// Returns a mutable reference to the list data.
    #[inline]
    pub fn data_mut(&mut self) -> &mut D {
        unsafe { &mut *self.data_ptr() }
    }

    #[allow(clippy::type_complexity)]
    #[inline(always)]
    #[allow(clippy::incompatible_msrv, unstable_name_collisions)]
    pub(crate) unsafe fn remove<F: FnOnce(Pin<&mut T>, &mut D) -> S>(
        &mut self,
        link: NonNull<NodeLink<L>>,
        new_state_if_last_node: F,
        is_front: bool,
        is_back: bool,
        is_cursor: bool,
    ) -> (Option<NonNull<NodeLink<L>>>, Option<NonNull<NodeLink<L>>>) {
        debug_assert!(is_front as usize + is_back as usize + is_cursor as usize <= 1);
        // For self-removal with AtomicLazy linking, the tail may not have been acquired (it was
        // written with at least Release in push_back, but a fence(Acquire) would not work as the
        // task may have moved in another thread)
        if L::NODES_ACCESS_REQUIRES_TAIL_ACQUIRE && !is_front && !is_back && !is_cursor {
            self.list.tail();
        }
        let link_ref = unsafe { link.as_ref() };
        let prev = if is_front {
            NonNull::new(ptr::without_provenance_mut(HEAD_MARKER)).unwrap()
        } else {
            // SAFETY: node is linked
            unsafe { link_ref.load_prev() }
        };
        let is_head = prev.addr().get() == HEAD_MARKER;
        let prev_next = if is_head {
            &self.list.head
        } else {
            unsafe { &prev.as_ref().next }
        };
        // A node retrieved from the tail might not have finished its insertion,
        // so it must be waited before overriding the chaining.
        if is_back || is_cursor {
            L::wait_next(prev_next, &self.list.parker);
        }
        let mut next = if is_back {
            None
        } else {
            L::load_next(&link_ref.next)
        };
        let mut tail = None;
        if next.is_none() {
            L::update_next(prev_next, None);
            let new_tail = if is_head {
                let data = unsafe { Pin::new_unchecked(&mut *NodeLink::data_ptr::<T>(link)) };
                // As `prev_next` may borrow `self.head`, list_data must not invalidate the borrow
                // and uses raw data pointer.
                let list_data = unsafe { &mut *self.data_ptr() };
                new_state_if_last_node(data, list_data).into_tail()
            } else {
                prev.into_tail()
            };
            let node_ptr = link.into_tail();
            let result = if L::SERIALIZED {
                self.list.tail.store(new_tail, Release);
                Ok(node_ptr)
            } else {
                (self.list.tail).compare_exchange(node_ptr, new_tail, Release, Relaxed)
            };
            if let Err(t) = result {
                if is_back || L::NODES_ACCESS_REQUIRES_TAIL_ACQUIRE {
                    fence(Acquire);
                }
                let tail = if is_front || is_back || is_cursor {
                    Some(unsafe { t.ptr().unwrap_unchecked() })
                } else {
                    // If the node is currently drained, the tail can be anything
                    t.ptr()
                };
                // If the node is drained, backward iteration can be started from it directly
                next = Some(self.get_next(Some(link), &link_ref.next, tail.unwrap_or(link)));
            } else if !is_head {
                tail = Some(prev);
            }
        }
        if let Some(next) = next {
            unsafe { next.as_ref().prev.store(prev.as_ptr(), Relaxed) };
            L::update_next(prev_next, Some(next));
        }
        link_ref.unlink();
        (next, tail)
    }
}

impl<'a, T, S: ListState, D, M: Mutex> LockedList<'a, T, S, D, Serialized, M> {
    /// Pushes the node at the back of the list.
    ///
    /// If the list is empty, its state is updated with the ordering `order`.
    #[inline]
    pub fn push_back<LR>(&mut self, node: NodeUnlinked<'_, LR>, order: Ordering)
    where
        LR: ListRef<NodeData = T, ListState = S, ListData = D, Linking = Serialized, Mutex = M>,
    {
        let head_marker = NonNull::new(ptr::without_provenance_mut(HEAD_MARKER)).unwrap();
        let prev = self.list.tail().unwrap_or(head_marker);
        unsafe { self.insert_between(node, prev, None, order) };
    }

    // TODO safety: `prev` is `HEAD_MARKER` or a linked node, `next` is `prev`'s successor
    #[inline(always)]
    #[allow(clippy::incompatible_msrv, unstable_name_collisions)]
    pub(super) unsafe fn insert_between<LR>(
        &mut self,
        node: NodeUnlinked<'_, LR>,
        prev: NonNull<NodeLink<Serialized>>,
        next: Option<NonNull<NodeLink<Serialized>>>,
        order: Ordering,
    ) where
        LR: ListRef<NodeData = T, ListState = S, ListData = D, Linking = Serialized, Mutex = M>,
    {
        debug_assert!(ptr::eq(node.list().as_list(), self.list));
        let mut link = node.link();
        let link_ref = unsafe { link.as_mut() };
        link_ref.prev.store_mut(prev.as_ptr());
        Serialized::update_next_mut(&mut link_ref.next, next);
        let is_head = prev.addr().get() == HEAD_MARKER;
        let prev_next = if is_head {
            &self.list.head
        } else {
            unsafe { &prev.as_ref().next }
        };
        Serialized::update_next(prev_next, Some(link));
        match next {
            Some(next) => unsafe { next.as_ref().prev.store(link.as_ptr(), Relaxed) },
            None => (self.list).store_tail_serialized(link.into_tail(), order, is_head),
        }
        node.set_linked(self.list);
    }
}

impl<'a, T, D, L: Linking, M: Mutex> LockedList<'a, T, (), D, L, M> {
    /// Detaches all the nodes of the list into a [`Drain`].
    #[inline]
    pub fn drain(self) -> Drain<'a, T, (), D, L, M> {
        Drain::new(self, |_| ())
    }
}

impl<'a, T, D, L: Linking, M: Mutex> LockedList<'a, T, usize, D, L, M> {
    /// See [`List::load_state`].
    #[inline]
    pub fn load_state(&self, order: Ordering) -> Option<usize> {
        self.list.load_state(order)
    }

    /// See [`List::compare_exchange_state`].
    #[inline]
    pub fn compare_exchange_state(
        &mut self,
        current: usize,
        new: usize,
        success: Ordering,
        failure: Ordering,
    ) -> Result<usize, Option<usize>> {
        if !L::SERIALIZED {
            return (self.list).compare_exchange_state_atomic(current, new, success, failure);
        }
        match self.list.tail.load(failure).state() {
            Some(state) if state == current => {
                self.list
                    .store_tail_serialized(new.into_tail(), success, true);
                Ok(current)
            }
            state => Err(state),
        }
    }

    /// See [`List::try_update_state`].
    #[inline]
    pub fn try_update_state<F: FnMut(usize) -> Option<usize>>(
        &mut self,
        set_order: Ordering,
        fetch_order: Ordering,
        mut f: F,
    ) -> Result<usize, Option<usize>> {
        if !L::SERIALIZED {
            return self.list.try_update_state_atomic(set_order, fetch_order, f);
        }
        let state = self.list.tail.load(fetch_order).state().ok_or(None)?;
        let new_state = f(state).ok_or(Some(state))?;
        (self.list).store_tail_serialized(new_state.into_tail(), set_order, true);
        Ok(state)
    }

    /// Detaches all the nodes of the list into a [`Drain`], storing the state returned by
    /// `new_state_if_not_empty` in the list if it is not empty.
    pub fn drain<F: FnOnce(&mut D) -> usize>(
        self,
        new_state_if_not_empty: F,
    ) -> Drain<'a, T, usize, D, L, M> {
        Drain::new(self, new_state_if_not_empty)
    }
}

impl<T, S: ListState, D, L: Linking, M: Mutex> Drop for LockedList<'_, T, S, D, L, M> {
    #[inline]
    fn drop(&mut self) {
        // Aborting on unwinding is not necessary, as a dropped node is unlinked at this point
        unsafe { self.list.mutex.unlock(ManuallyDrop::take(&mut self.guard)) };
    }
}

mod private {
    pub trait PrivateEnd {}
}
pub(crate) use private::PrivateEnd;

/// An end of a [`List`] or a [`Drain`], either [`Front`] or [`Back`].
pub trait End: PrivateEnd {
    /// `true` for [`Front`].
    const IS_FRONT: bool;
    /// `true` for [`Back`].
    const IS_BACK: bool;
}

/// The front end of a [`List`] or a [`Drain`].
pub struct Front;
/// The back end of a [`List`] or a [`Drain`].
pub struct Back;

impl PrivateEnd for Front {}
impl End for Front {
    const IS_FRONT: bool = true;
    const IS_BACK: bool = false;
}

impl PrivateEnd for Back {}
impl End for Back {
    const IS_FRONT: bool = false;
    const IS_BACK: bool = true;
}

/// An end of a locked [`List`], either its [`ListFront`] or its [`ListBack`].
pub struct ListEnd<
    'locked,
    'a,
    E: End,
    T,
    S: ListState = (),
    D = (),
    L: Linking = AtomicEager,
    M: Mutex = DefaultMutex,
> {
    node: NonNull<NodeLink<L>>,
    locked: &'a mut LockedList<'locked, T, S, D, L, M>,
    _end: PhantomData<E>,
}

/// The front node of a locked [`List`], obtained from [`LockedList::front`].
pub type ListFront<'locked, 'a, T, S = (), D = (), L = AtomicEager, M = DefaultMutex> =
    ListEnd<'locked, 'a, Front, T, S, D, L, M>;

/// The back node of a locked [`List`], obtained from [`LockedList::back`].
pub type ListBack<'locked, 'a, T, S = (), D = (), L = AtomicEager, M = DefaultMutex> =
    ListEnd<'locked, 'a, Back, T, S, D, L, M>;

unsafe impl<'locked, E: End, T: Send, S: ListState, D, L: Linking, M: Mutex> Send
    for ListEnd<'locked, '_, E, T, S, D, L, M>
where
    LockedList<'locked, T, S, D, L, M>: Send,
{
}
unsafe impl<'locked, E: End, T: Sync, S: ListState, D, L: Linking, M: Mutex> Sync
    for ListEnd<'locked, '_, E, T, S, D, L, M>
where
    LockedList<'locked, T, S, D, L, M>: Sync,
{
}

impl<'locked, 'a, E: End, T, S: ListState, D, L: Linking, M: Mutex>
    ListEnd<'locked, 'a, E, T, S, D, L, M>
{
    #[inline]
    fn unlink_impl<F: FnOnce(Pin<&mut T>, &mut D) -> S>(
        self,
        new_state_if_last_node: F,
    ) -> Option<Self> {
        let (next, tail) = unsafe {
            (self.locked).remove(
                self.node,
                new_state_if_last_node,
                E::IS_FRONT,
                E::IS_BACK,
                false,
            )
        };
        Some(Self {
            node: if E::IS_FRONT { next? } else { tail? },
            locked: self.locked,
            _end: PhantomData,
        })
    }

    /// Returns a cursor pointing to the node.
    #[inline]
    pub fn into_cursor(self) -> ListCursor<'locked, 'a, T, S, D, L, M> {
        ListCursor::new(Some(self.node), self.locked)
    }
}

impl<E: End, T, D, L: Linking, M: Mutex> ListEnd<'_, '_, E, T, (), D, L, M> {
    /// Unlinks the node, returning the new end of the list, `None` if it becomes empty.
    ///
    /// With atomic linkings, the new back may be a node pushed after the unlinked one; use a
    /// [`Drain`] to walk a fixed set of nodes.
    #[inline]
    pub fn unlink(self) -> Option<Self> {
        self.unlink_impl(|_, _| ())
    }
}

impl<E: End, T, D, L: Linking, M: Mutex> ListEnd<'_, '_, E, T, usize, D, L, M> {
    /// Unlinks the node, returning the new end of the list, `None` if it becomes empty.
    ///
    /// If the node was the last remaining one, the list state is updated with
    /// `new_state_if_last_node`.
    ///
    /// With atomic linkings, the new back may be a node pushed after the unlinked one; use a
    /// [`Drain`] to walk a fixed set of nodes.
    #[inline]
    pub fn unlink<F: FnOnce(Pin<&mut T>, &mut D) -> usize>(
        self,
        new_state_if_last_node: F,
    ) -> Option<Self> {
        self.unlink_impl(new_state_if_last_node)
    }
}

node_ref!(
    ListEnd<'locked, 'a, E: End, T, S: ListState, D, L: Linking, M: Mutex>,
    (T, L, D),
    (self.node),
    (self.locked)
);
