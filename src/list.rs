use core::{cell::UnsafeCell, marker::PhantomData, mem::ManuallyDrop, pin::Pin, ptr, ptr::NonNull};

use crate::{
    loom::{
        AtomicPtrExt,
        sync::atomic::{AtomicPtr, Ordering, Ordering::*, fence},
    },
    node::{LinkedNodeRef, NodeData, NodeLink, NodeRef, NodeUnlinked, PrivateNodeRef, node_ref},
    sync::mutex::{DefaultMutex, Mutex},
    utils::abort_on_unwind,
};

mod cursor;
mod drain;
mod linking;
pub(crate) mod state;

pub use cursor::*;
pub use drain::*;
pub use linking::*;
pub use state::*;

type MutexGuard<'a, M> = <M as Mutex>::Guard<'a>;

const HEAD_MARKER: usize = 1;

pub struct List<T, S: ListState = (), D = (), L: Linking = AtomicEager, M: Mutex = DefaultMutex> {
    tail: AtomicPtr<Tail<S, L>>,
    head: L::NextPtr,
    mutex: M,
    parker: L::Parker,
    data: UnsafeCell<D>,
    // TODO same trick as `NodeInner::access`
    #[cfg(loom)]
    data_access: crate::loom::cell::Cell<()>,
    _node_data: PhantomData<T>,
}

unsafe impl<T: Send, S: ListState, D: Send, L: Linking, M: Mutex> Send for List<T, S, D, L, M> {}
unsafe impl<T: Send, S: ListState, D: Send, L: Linking, M: Mutex> Sync for List<T, S, D, L, M> {}

impl<T, S: ListState, L: Linking, M: Mutex> List<T, S, (), L, M> {
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

    #[cfg_attr(loom, const_fn::const_fn(cfg(false)))]
    #[inline]
    pub const fn with_data(data: D) -> Self {
        Self::new_impl(ptr::null_mut(), data)
    }

    #[inline]
    pub const fn data_mut(&mut self) -> &mut D {
        self.data.get_mut()
    }

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
    fn store_tail_serialized(&self, new_tail: *mut Tail<S, L>, order: Ordering) {
        debug_assert!(L::SERIALIZED);
        match order {
            Acquire | AcqRel => {
                self.tail.swap(new_tail, order);
            }
            _ => self.tail.store(new_tail, order),
        }
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
    pub fn lock(&self) -> LockedList<'_, T, S, D, L, M> {
        LockedList {
            list: self,
            guard: ManuallyDrop::new(abort_on_unwind(|| self.mutex.lock())),
            _not_send: PhantomData,
        }
    }

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
        let mut tail = self.tail.load(fetch_order);
        let prev = loop {
            let (new_tail, prev) = match S::tail_to_enum(tail) {
                StateOrPtr::State(state)
                    if let Some(f) = f.as_mut()
                        && let Some(new_state) = f(node.data_mut(), state) =>
                {
                    (new_state.into_tail(), ptr::null_mut())
                }
                state_or_ptr if !on_push(node.data_mut(), state_or_ptr.state()) => {
                    unsafe { link.as_mut().prev.store_mut(ptr::null_mut()) };
                    return Err(false);
                }
                StateOrPtr::State(_) => {
                    (link.into_tail(), ptr::without_provenance_mut(HEAD_MARKER))
                }
                StateOrPtr::Ptr(prev) => (link.into_tail(), prev.as_ptr()),
            };
            unsafe { link.as_mut().prev.store_mut(prev) };
            if L::SERIALIZED {
                self.store_tail_serialized(new_tail, set_order);
                break prev;
            }
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
        L::store_next(prev_next, link, &self.parker);
        node.set_linked(self);
        Err(true)
    }
}

impl<T, L: Linking, M: Mutex> List<T, usize, (), L, M> {
    #[cfg_attr(loom, const_fn::const_fn(cfg(false)))]
    #[inline]
    pub const fn with_state(state: usize) -> Self {
        Self::new_impl(state_to_ptr(state), ())
    }
}

impl<T, D, L: Linking, M: Mutex> List<T, usize, D, L, M> {
    #[cfg_attr(loom, const_fn::const_fn(cfg(false)))]
    #[inline]
    pub const fn with_state_and_data(state: usize, data: D) -> Self {
        Self::new_impl(state_to_ptr(state), data)
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
    pub fn get_state(&mut self) -> Option<usize> {
        self.tail.load_mut().state()
    }

    #[inline]
    pub fn try_set_state(&mut self, state: usize) -> bool {
        if self.get_state().is_none() {
            return false;
        }
        self.tail.store_mut(state.into_tail());
        true
    }

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
    ) -> Result<usize, LockedList<'_, T, usize, D, L, M>> {
        if !L::SERIALIZED
            && let Ok(s) = self.try_update_state_atomic(set_order, fetch_order, |s| Some(f(s)))
        {
            return Ok(s);
        }
        let mut locked = self.lock();
        locked
            .try_update_state(set_order, fetch_order, |s| Some(f(s)))
            .or(Err(locked))
    }

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

pub trait ListRef {
    type NodeData: NodeData<Self>;
    type ListState: ListState;
    type ListData;
    type Linking: Linking;
    type Mutex: Mutex;

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
    #[inline]
    pub fn is_empty(&self, order: Ordering) -> bool {
        self.list.is_empty(order)
    }

    #[inline]
    pub fn is_empty_rmw(&self, order: Ordering) -> bool {
        self.list.is_empty_rmw(order)
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

    #[inline]
    pub fn front(&mut self) -> Option<ListFront<'a, '_, T, S, D, L, M>>
    where
        L: Linking,
    {
        let node = if L::SERIALIZED {
            L::load_next(&self.list.head)?
        } else {
            self.get_next(None, &self.list.head, self.list.tail()?)
        };
        Some(ListFront { node, locked: self })
    }

    #[inline]
    pub fn back(&mut self) -> Option<ListBack<'a, '_, T, S, D, L, M>> {
        let node = self.list.tail()?;
        Some(ListBack { node, locked: self })
    }

    #[inline]
    pub fn cursor_front(&mut self) -> ListCursor<'a, '_, T, S, D, L, M> {
        ListCursor::new(self.front().map(|f| f.node), self)
    }

    #[inline]
    pub fn cursor_back(&mut self) -> ListCursor<'a, '_, T, S, D, L, M> {
        ListCursor::new(self.back().map(|t| t.node), self)
    }

    pub fn unlock(self) -> &'a List<T, S, D, L, M> {
        self.list
    }

    // TODO same trick as `NodeLink::data_ptr` for loom
    #[inline(always)]
    pub(crate) fn data_ptr(&self) -> *mut D {
        #[cfg(loom)]
        self.list.data_access.set(());
        self.list.data.get()
    }

    #[inline]
    pub fn data(&self) -> &D {
        unsafe { &*self.data_ptr() }
    }

    #[inline]
    pub fn data_mut(&mut self) -> &mut D {
        unsafe { &mut *self.data_ptr() }
    }

    #[allow(clippy::type_complexity)]
    #[inline(always)]
    pub(crate) unsafe fn remove<F: FnOnce(Pin<&mut T>, &mut D) -> S>(
        &mut self,
        link: NonNull<NodeLink<L>>,
        new_state_if_last_node: F,
        is_front: bool,
        is_back: bool,
        is_cursor: bool,
    ) -> (Option<NonNull<NodeLink<L>>>, Option<NonNull<NodeLink<L>>>) {
        debug_assert!(is_front as usize + is_back as usize + is_cursor as usize <= 1);
        // TODO for self-removal with LazyDoubly/Singly, the tail may not have been acquired
        // (it was written with at least Release in push_back, but a fence(Acquire) would not
        // work as the task may have moved in another thread)
        if L::NODES_ACCESS_REQUIRES_TAIL_ACQUIRE && !is_front && !is_back && !is_cursor {
            self.list.tail();
        }
        let link_ref = unsafe { link.as_ref() };
        let prev = if is_front {
            NonNull::new(ptr::without_provenance_mut(HEAD_MARKER)).unwrap()
        } else {
            // TODO safety the node is linked
            unsafe { link_ref.load_prev() }
        };
        let is_head = prev.addr().get() == HEAD_MARKER;
        let prev_next = if is_head {
            &self.list.head
        } else {
            unsafe { &prev.as_ref().next }
        };
        // TODO a cursor node may come from the tail or a `prev` walk, so its incoming edge may
        // still be unpublished, unlike a node returned by `get_next`
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
                // TODO raw pointers: `prev_next` may borrow `self.head`
                let data = unsafe { Pin::new_unchecked(&mut *NodeLink::data_ptr::<T>(link)) };
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
                    // TODO if the node is currently drained, the tail can be anything
                    t.ptr()
                };
                // TODO is the node is drained, backward iteration can be started from it directly
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
        let prev_next = if prev.addr().get() == HEAD_MARKER {
            &self.list.head
        } else {
            unsafe { &prev.as_ref().next }
        };
        Serialized::update_next(prev_next, Some(link));
        match next {
            Some(next) => unsafe { next.as_ref().prev.store(link.as_ptr(), Relaxed) },
            None => self.list.store_tail_serialized(link.into_tail(), order),
        }
        node.set_linked(self.list);
    }
}

impl<'a, T, D, L: Linking, M: Mutex> LockedList<'a, T, (), D, L, M> {
    #[inline]
    pub fn drain(self) -> Drain<'a, T, (), D, L, M> {
        Drain::new(self, |_| ())
    }
}

impl<'a, T, D, L: Linking, M: Mutex> LockedList<'a, T, usize, D, L, M> {
    #[inline]
    pub fn load_state(&self, order: Ordering) -> Option<usize> {
        self.list.load_state(order)
    }

    #[inline]
    pub fn load_state_or(&self, order: Ordering, default: usize) -> usize {
        self.list.load_state_or(order, default)
    }

    #[inline]
    pub fn load_state_rmw(&self, order: Ordering) -> Option<usize> {
        self.list.load_state_rmw(order)
    }

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
                self.list.store_tail_serialized(new.into_tail(), success);
                Ok(current)
            }
            state => Err(state),
        }
    }

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
        (self.list).store_tail_serialized(new_state.into_tail(), set_order);
        Ok(state)
    }

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
        // TODO aborting on unwinding is not necessary as nodes needing to be released have
        // been released
        unsafe { self.list.mutex.unlock(ManuallyDrop::take(&mut self.guard)) };
    }
}

pub trait ListEnd<
    'locked,
    'a,
    T,
    S: ListState = (),
    D = (),
    L: Linking = AtomicEager,
    M: Mutex = DefaultMutex,
>: LinkedNodeRef<T, D> + Sized
{
    fn unlink<F: FnOnce(Pin<&mut T>, &mut D) -> S>(self, new_state_if_last_node: F)
    -> Option<Self>;
}

pub struct ListFront<
    'locked,
    'a,
    T,
    S: ListState = (),
    D = (),
    L: Linking = AtomicEager,
    M: Mutex = DefaultMutex,
> {
    node: NonNull<NodeLink<L>>,
    locked: &'a mut LockedList<'locked, T, S, D, L, M>,
}

unsafe impl<'locked, T: Send, S: ListState, D, L: Linking, M: Mutex> Send
    for ListFront<'locked, '_, T, S, D, L, M>
where
    LockedList<'locked, T, S, D, L, M>: Send,
{
}
unsafe impl<'locked, T: Sync, S: ListState, D, L: Linking, M: Mutex> Sync
    for ListFront<'locked, '_, T, S, D, L, M>
where
    LockedList<'locked, T, S, D, L, M>: Sync,
{
}

impl<'locked, 'a, T, S: ListState, D, L: Linking, M: Mutex> ListEnd<'locked, 'a, T, S, D, L, M>
    for ListFront<'locked, 'a, T, S, D, L, M>
{
    #[inline]
    fn unlink<F: FnOnce(Pin<&mut T>, &mut D) -> S>(
        self,
        new_state_if_last_node: F,
    ) -> Option<Self> {
        let (next, _) =
            unsafe { (self.locked).remove(self.node, new_state_if_last_node, true, false, false) };
        Some(Self {
            node: next?,
            locked: self.locked,
        })
    }
}

impl<T, D, L: Linking, M: Mutex> ListFront<'_, '_, T, (), D, L, M> {
    pub fn unlink(self) -> Option<Self> {
        ListEnd::unlink(self, |_, _| ())
    }
}

impl<T, D, L: Linking, M: Mutex> ListFront<'_, '_, T, usize, D, L, M> {
    pub fn unlink<F: FnOnce(Pin<&mut T>, &mut D) -> usize>(
        self,
        new_state_if_last_node: F,
    ) -> Option<Self> {
        ListEnd::unlink(self, new_state_if_last_node)
    }
}

node_ref!(
    ListFront<'locked, 'a, T, S: ListState, D, L: Linking, M: Mutex>,
    (T, L, D),
    (self.node),
    (self.locked)
);

pub struct ListBack<
    'locked,
    'a,
    T,
    S: ListState = (),
    D = (),
    L: Linking = AtomicEager,
    M: Mutex = DefaultMutex,
> {
    node: NonNull<NodeLink<L>>,
    locked: &'a mut LockedList<'locked, T, S, D, L, M>,
}

unsafe impl<'locked, T: Send, S: ListState, D, L: Linking, M: Mutex> Send
    for ListBack<'locked, '_, T, S, D, L, M>
where
    LockedList<'locked, T, S, D, L, M>: Send,
{
}
unsafe impl<'locked, T: Sync, S: ListState, D, L: Linking, M: Mutex> Sync
    for ListBack<'locked, '_, T, S, D, L, M>
where
    LockedList<'locked, T, S, D, L, M>: Sync,
{
}

impl<'locked, 'a, T, S: ListState, D, L: Linking, M: Mutex> ListEnd<'locked, 'a, T, S, D, L, M>
    for ListBack<'locked, 'a, T, S, D, L, M>
{
    #[inline]
    fn unlink<F: FnOnce(Pin<&mut T>, &mut D) -> S>(
        self,
        new_state_if_last_node: F,
    ) -> Option<Self> {
        let (_, tail) =
            unsafe { (self.locked).remove(self.node, new_state_if_last_node, false, true, false) };
        Some(Self {
            node: tail?,
            locked: self.locked,
        })
    }
}

impl<T, D, L: Linking, M: Mutex> ListBack<'_, '_, T, (), D, L, M> {
    pub fn unlink(self) -> Option<Self> {
        ListEnd::unlink(self, |_, _| ())
    }
}

impl<T, D, L: Linking, M: Mutex> ListBack<'_, '_, T, usize, D, L, M> {
    pub fn unlink<F: FnOnce(Pin<&mut T>, &mut D) -> usize>(
        self,
        new_state_if_last_node: F,
    ) -> Option<Self> {
        ListEnd::unlink(self, new_state_if_last_node)
    }
}

node_ref!(
    ListBack<'locked, 'a, T, S: ListState, D, L: Linking, M: Mutex>,
    (T, L, D),
    (self.node),
    (self.locked)
);

pub struct GetFront;
pub struct GetBack;

pub trait ListGetEnd {
    type ListEnd<'locked, 'a, T, S: ListState, D, L: Linking, M: Mutex>: ListEnd<'locked, 'a, T, S, D, L, M>
    where
        'locked: 'a,
        T: 'locked,
        D: 'locked;

    fn get_end<'locked, 'a, T, S: ListState, D, L: Linking, M: Mutex>(
        locked: &'a mut LockedList<'locked, T, S, D, L, M>,
    ) -> Option<Self::ListEnd<'locked, 'a, T, S, D, L, M>>;
}

impl ListGetEnd for GetFront {
    type ListEnd<'locked, 'a, T, S: ListState, D, L: Linking, M: Mutex>
        = ListFront<'locked, 'a, T, S, D, L, M>
    where
        'locked: 'a,
        T: 'locked,
        D: 'locked;

    #[inline]
    fn get_end<'locked, 'a, T, S: ListState, D, L: Linking, M: Mutex>(
        locked: &'a mut LockedList<'locked, T, S, D, L, M>,
    ) -> Option<Self::ListEnd<'locked, 'a, T, S, D, L, M>> {
        locked.front()
    }
}

impl ListGetEnd for GetBack {
    type ListEnd<'locked, 'a, T, S: ListState, D, L: Linking, M: Mutex>
        = ListBack<'locked, 'a, T, S, D, L, M>
    where
        'locked: 'a,
        T: 'locked,
        D: 'locked;

    #[inline]
    fn get_end<'locked, 'a, T, S: ListState, D, L: Linking, M: Mutex>(
        locked: &'a mut LockedList<'locked, T, S, D, L, M>,
    ) -> Option<Self::ListEnd<'locked, 'a, T, S, D, L, M>> {
        locked.back()
    }
}
