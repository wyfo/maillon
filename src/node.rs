#[cfg(nightly)]
use core::pin::UnsafePinned;
use core::{marker::PhantomData, pin::Pin, ptr, ptr::NonNull};

#[cfg(not(nightly))]
use crate::unsafe_pinned::UnsafePinned;
use crate::{
    List,
    list::{AsList, Eager, Linking, ListState, LockedList},
    loom::{
        cell::Cell,
        sync::atomic::{AtomicPtr, Ordering, Ordering::*},
    },
    sync::mutex::{DefaultMutex, Mutex},
};

pub trait NodeData<LR, S: ListState = (), L: Linking = Eager, M: Mutex = DefaultMutex>:
    Sized
{
    fn new_state_if_last_node_on_drop(self: Pin<&mut Self>, list: &LR) -> S;
    fn on_drop<'list>(
        self: Pin<&mut Self>,
        list: &'list LR,
        locked: Option<LockedList<'list, Self, S, L, M>>,
        state_updated_on_unlink: bool,
    );
}

#[repr(align(4))]
pub(crate) struct NodeLink<L: PrivateLinking> {
    pub(crate) prev: AtomicPtr<NodeLink<L>>,
    pub(crate) next: L::NextPtr,
}

impl<L: Linking> NodeLink<L> {
    #[cfg_attr(loom, const_fn::const_fn(cfg(false)))]
    pub(crate) const fn new() -> Self {
        Self {
            prev: AtomicPtr::new(ptr::null_mut()),
            #[cfg(not(loom))]
            next: L::NEW_NEXT,
            #[cfg(loom)]
            next: L::new_next(None),
        }
    }

    #[inline(always)]
    pub(crate) fn is_linked(&self) -> bool {
        !self.prev.load(Acquire).is_null()
    }
}

#[repr(C)]
pub(crate) struct NodeInner<T, L: Linking> {
    pub(crate) link: NodeLink<L>,
    pub(crate) data: T,
    // TODO
    /// Dummy cell, whose only purpose is to report data accesses to loom: the real accesses go
    /// through raw pointers, which loom cannot see. `data_ptr`/`data_ptr_mut` register a shared
    /// resp. exclusive access on it, so a missing happens-before edge is still detected.
    #[cfg(loom)]
    pub(crate) access: Cell<()>,
}

pub enum NodeState<
    'a,
    LR: AsList<List<T, S, L, M>>,
    T: NodeData<LR, S, L, M>,
    S: ListState,
    L: Linking,
    M: Mutex,
> {
    Unlinked(NodeUnlinked<'a, LR, T, S, L, M>),
    Linked(NodeLinked<'a, LR, T, S, L, M>),
}

pub struct Node<
    LR: AsList<List<T, S, L, M>>,
    T: NodeData<LR, S, L, M>,
    S: ListState = (),
    L: Linking = Eager,
    M: Mutex = DefaultMutex,
> {
    list: LR,
    node: UnsafePinned<NodeInner<T, L>>,
    maybe_linked: Cell<bool>,
    _state: PhantomData<S>,
    _sync: PhantomData<(L, M)>,
}

unsafe impl<
    LR: AsList<List<T, S, L, M>> + Send,
    T: NodeData<LR, S, L, M> + Send,
    S: ListState,
    L: Linking,
    M: Mutex,
> Send for Node<LR, T, S, L, M>
{
}
unsafe impl<
    LR: AsList<List<T, S, L, M>> + Sync,
    T: NodeData<LR, S, L, M>,
    S: ListState,
    L: Linking,
    M: Mutex,
> Sync for Node<LR, T, S, L, M>
{
}

impl<LR: AsList<List<T, S, L, M>>, T: NodeData<LR, S, L, M>, S: ListState, L: Linking, M: Mutex>
    Node<LR, T, S, L, M>
{
    pub fn new(list: LR) -> Self
    where
        T: Default,
    {
        Self::with_data(list, Default::default())
    }

    #[cfg_attr(loom, const_fn::const_fn(cfg(false)))]
    pub const fn with_data(list: LR, data: T) -> Self {
        Self {
            list,
            node: UnsafePinned::new(NodeInner {
                link: NodeLink::new(),
                data,
                #[cfg(loom)]
                access: loom::cell::Cell::new(()),
            }),
            maybe_linked: Cell::new(false),
            _state: PhantomData,
            _sync: PhantomData,
        }
    }

    #[inline(always)]
    pub const fn list(&self) -> &LR {
        &self.list
    }

    fn link(&self) -> NonNull<NodeLink<L>> {
        NonNull::new(self.node.get()).unwrap().cast()
    }

    // TODO doc: false = never pushed, or already observed unlinked -> nothing set by the list
    // (notification etc.) can be pending; true = may be linked, or unlinked since by another
    // thread. Set by push_back, cleared by state()/unlink. Plain Cell read, no atomic, no lock.
    // Not the negation of is_linked, which is authoritative both ways.
    #[inline(always)]
    pub fn is_maybe_linked(&self) -> bool {
        self.maybe_linked.get()
    }

    #[inline(always)]
    pub fn is_linked(&self) -> bool {
        unsafe { (*self.node.get()).link.is_linked() }
    }

    #[inline(always)]
    pub fn state(self: Pin<&mut Self>) -> NodeState<'_, LR, T, S, L, M> {
        let this = self.into_ref().get_ref();
        if this.is_maybe_linked() {
            if this.is_linked() {
                let locked = this.list.as_list().lock();
                if this.is_linked() {
                    return NodeState::Linked(NodeLinked { node: this, locked });
                }
            }
            this.maybe_linked.set(false);
        }
        NodeState::Unlinked(NodeUnlinked(this))
    }

    #[cold]
    #[inline(never)]
    fn drop_linked(&mut self) {
        let mut locked = self.list.as_list().lock();
        let mut node = NodeDropped(self);
        let mut state_updated = false;
        if self.is_linked() {
            let new_state = || node.data_mut().new_state_if_last_node_on_drop(&self.list);
            let (next, tail) = unsafe { locked.remove(self.link(), new_state, false, false) };
            state_updated = next.is_none() && tail.is_none();
        }
        (node.data_mut()).on_drop(&self.list, Some(locked), state_updated);
    }
}

impl<LR: AsList<List<T, S, L, M>>, T: NodeData<LR, S, L, M>, S: ListState, L: Linking, M: Mutex>
    Drop for Node<LR, T, S, L, M>
{
    #[inline]
    fn drop(&mut self) {
        if self.is_maybe_linked() && self.is_linked() {
            self.drop_linked();
        } else {
            (NodeDropped(self).data_mut()).on_drop(&self.list, None, false);
        }
    }
}

pub struct NodeUnlinked<
    'a,
    LR: AsList<List<T, S, L, M>>,
    T: NodeData<LR, S, L, M>,
    S: ListState = (),
    L: Linking = Eager,
    M: Mutex = DefaultMutex,
>(&'a Node<LR, T, S, L, M>);

unsafe impl<
    LR: AsList<List<T, S, L, M>> + Sync,
    T: NodeData<LR, S, L, M> + Send,
    S: ListState,
    L: Linking,
    M: Mutex,
> Send for NodeUnlinked<'_, LR, T, S, L, M>
{
}
unsafe impl<
    LR: AsList<List<T, S, L, M>> + Sync,
    T: NodeData<LR, S, L, M> + Sync,
    S: ListState,
    L: Linking,
    M: Mutex,
> Sync for NodeUnlinked<'_, LR, T, S, L, M>
{
}

node_ref!(
    NodeUnlinked<
        'a,
        LR: AsList<List<T, S, L, M>>,
        T: NodeData<LR, S, L, M>,
        S: ListState,
        L: Linking,
        M: Mutex,
    >,
    T,
    L,
    self.0.link()
);

impl<'a, LR: AsList<List<T, S, L, M>>, T: NodeData<LR, S, L, M>, S: ListState, L: Linking, M: Mutex>
    NodeUnlinked<'a, LR, T, S, L, M>
{
    #[inline]
    pub fn list(&self) -> &'a LR {
        self.0.list()
    }
}

impl<'a, LR: AsList<List<T, (), L, M>>, T: NodeData<LR, (), L, M>, L: Linking, M: Mutex>
    NodeUnlinked<'a, LR, T, (), L, M>
{
    #[inline]
    pub fn push_back(self, order: Ordering) {
        let list = self.list().as_list();
        let link = self.0.link();
        let f = None::<fn(()) -> Option<()>>;
        let on_pushed = || self.0.maybe_linked.set(true);
        let _ = unsafe { list.push_back(link, order, Relaxed, f, |_| true, on_pushed) };
    }
}

impl<'a, LR: AsList<List<T, usize, L, M>>, T: NodeData<LR, usize, L, M>, L: Linking, M: Mutex>
    NodeUnlinked<'a, LR, T, usize, L, M>
{
    pub fn try_push_back_with<P: FnMut(Pin<&mut T>, Option<usize>) -> bool>(
        self,
        set_order: Ordering,
        fetch_order: Ordering,
        mut on_push: P,
    ) -> bool {
        let list = self.list().as_list();
        let link = self.0.link();
        let f = None::<fn(usize) -> Option<usize>>;
        let on_push_back = |state| on_push(Self(self.0).data_mut(), state);
        let on_pushed = || self.0.maybe_linked.set(true);
        unsafe { list.push_back(link, set_order, fetch_order, f, on_push_back, on_pushed) }
            .unwrap_err()
    }

    pub fn try_update_state_or_push_back_with<
        F: FnMut(Pin<&mut T>, usize) -> Option<usize>,
        P: FnMut(Pin<&mut T>, Option<usize>) -> bool,
        U: FnOnce(Pin<&mut T>, usize),
    >(
        mut self,
        set_order: Ordering,
        fetch_order: Ordering,
        mut f: F,
        on_state_updated: U,
        mut on_push: P,
    ) -> Result<usize, bool> {
        let list = self.list().as_list();
        let link = self.0.link();
        let f = |state| f(Self(self.0).data_mut(), state);
        let on_push = |state| on_push(Self(self.0).data_mut(), state);
        let on_pushed = || self.0.maybe_linked.set(true);
        unsafe { list.push_back(link, set_order, fetch_order, Some(f), on_push, on_pushed) }
            .inspect(|&state| on_state_updated(self.data_mut(), state))
    }
}

pub struct NodeLinked<
    'a,
    LR: AsList<List<T, S, L, M>>,
    T: NodeData<LR, S, L, M>,
    S: ListState = (),
    L: Linking = Eager,
    M: Mutex = DefaultMutex,
> {
    node: &'a Node<LR, T, S, L, M>,
    locked: LockedList<'a, T, S, L, M>,
}

unsafe impl<
    'a,
    LR: AsList<List<T, S, L, M>> + Sync,
    T: NodeData<LR, S, L, M> + Send,
    S: ListState,
    L: Linking,
    M: Mutex,
> Send for NodeLinked<'a, LR, T, S, L, M>
where
    LockedList<'a, T, S, L, M>: Send,
{
}
unsafe impl<
    'a,
    LR: AsList<List<T, S, L, M>> + Sync,
    T: NodeData<LR, S, L, M> + Sync,
    S: ListState,
    L: Linking,
    M: Mutex,
> Sync for NodeLinked<'a, LR, T, S, L, M>
where
    LockedList<'a, T, S, L, M>: Sync,
{
}

node_ref!(
    NodeLinked<
        'a,
        LR: AsList<List<T, S, L, M>>,
        T: NodeData<LR, S, L, M>,
        S: ListState,
        L: Linking,
        M: Mutex,
    >,
    T,
    L,
    self.node.link()
);

impl<'a, LR: AsList<List<T, S, L, M>>, T: NodeData<LR, S, L, M>, S: ListState, L: Linking, M: Mutex>
    NodeLinked<'a, LR, T, S, L, M>
{
    #[inline]
    pub fn list(&self) -> &'a LR {
        self.node.list()
    }
}

impl<'a, LR: AsList<List<T, (), L, M>>, T: NodeData<LR, (), L, M>, L: Linking, M: Mutex>
    NodeLinked<'a, LR, T, (), L, M>
{
    #[inline]
    #[allow(clippy::type_complexity)]
    pub fn unlink(
        mut self,
    ) -> (
        NodeUnlinked<'a, LR, T, (), L, M>,
        LockedList<'a, T, (), L, M>,
    ) {
        unsafe { self.locked.remove(self.node.link(), || (), false, false) };
        self.node.maybe_linked.set(false);
        (NodeUnlinked(self.node), self.locked)
    }
}

impl<'a, LR: AsList<List<T, usize, L, M>>, T: NodeData<LR, usize, L, M>, L: Linking, M: Mutex>
    NodeLinked<'a, LR, T, usize, L, M>
{
    #[inline]
    #[allow(clippy::type_complexity)]
    pub fn unlink<F: FnOnce() -> usize>(
        mut self,
        new_state_if_last_node: F,
    ) -> (
        NodeUnlinked<'a, LR, T, usize, L, M>,
        LockedList<'a, T, usize, L, M>,
        bool,
    ) {
        let (next, tail) = unsafe {
            self.locked
                .remove(self.node.link(), new_state_if_last_node, false, false)
        };
        let state_updated = next.is_none() && tail.is_none();
        self.node.maybe_linked.set(false);
        (NodeUnlinked(self.node), self.locked, state_updated)
    }
}

struct NodeDropped<
    'a,
    LR: AsList<List<T, S, L, M>>,
    T: NodeData<LR, S, L, M>,
    S: ListState,
    L: Linking,
    M: Mutex,
>(&'a Node<LR, T, S, L, M>);

node_ref!(
    NodeDropped<
        'a,
        LR: AsList<List<T, S, L, M>>,
        T: NodeData<LR, S, L, M>,
        S: ListState,
        L: Linking,
        M: Mutex,
    >,
    T,
    L,
    self.0.link()
);

pub(crate) mod private {
    use core::ptr::NonNull;

    use crate::{
        list::Linking,
        node::{NodeInner, NodeLink},
    };

    pub(crate) trait NodeRef {
        type Linking: Linking;

        fn node(&self) -> NonNull<NodeLink<Self::Linking>>;

        #[inline(always)]
        fn data_ptr<T>(&self) -> *mut T {
            let inner = self.node().as_ptr().cast::<NodeInner<T, Self::Linking>>();
            #[cfg(loom)]
            unsafe {
                (*inner).access.set(());
            }
            unsafe { &raw mut (*inner).data }
        }
    }
}

#[expect(private_bounds)]
pub trait NodeRef<T>: private::NodeRef {
    #[inline]
    fn data(&self) -> &T {
        unsafe { &*self.data_ptr::<T>() }
    }

    #[inline]
    fn data_mut(&mut self) -> Pin<&mut T> {
        unsafe { Pin::new_unchecked(&mut *self.data_ptr::<T>()) }
    }
}

macro_rules! node_ref {
    ($ty:ident<$($lf:lifetime,)* $($arg:ident $(:$bound:path)?),* $(,)?>, $data:ty, $linking:ty, self.$($node_path:tt)*) => {
        impl<$($lf,)* $($arg $(:$bound)?),*> crate::node::private::NodeRef
            for $ty<$($lf,)* $($arg),*>
        {
            type Linking = $linking;

            #[inline(always)]
            fn node(&self) -> core::ptr::NonNull<crate::node::NodeLink<$linking>> {
                self.$($node_path)*
            }
        }

        impl<$($lf,)* $($arg $(:$bound)?),*> crate::node::NodeRef<$data>
            for $ty<$($lf,)* $($arg),*>
        {
        }

        impl<$($lf,)* $($arg $(:$bound)?),*> core::ops::Deref for $ty<$($lf,)* $($arg),*> {
            type Target = $data;
            #[inline]
            fn deref(&self) -> &Self::Target {
                crate::node::NodeRef::data(self)
            }
        }

        impl<$($lf,)* $($arg $(:$bound)?),*> core::ops::DerefMut for $ty<$($lf,)* $($arg),*>
        where
            Self::Target: Unpin
        {
            #[inline]
            fn deref_mut(&mut self) -> &mut Self::Target {
                crate::node::NodeRef::data_mut(self).get_mut()
            }
        }
    };
}
pub(crate) use node_ref;

use crate::list::PrivateLinking;
