#[cfg(nightly)]
use core::pin::UnsafePinned;
use core::{marker::PhantomData, pin::Pin, ptr, ptr::NonNull};

#[cfg(not(nightly))]
use crate::unsafe_pinned::UnsafePinned;
use crate::{
    List,
    list::{AsList, ListState, LockedList},
    loom::{
        cell::Cell,
        sync::atomic::{AtomicPtr, Ordering, Ordering::*},
    },
    sync::{DefaultSyncPrimitives, SyncPrimitives},
};

pub(crate) const NULL: *mut NodeLink = ptr::null_mut();

pub trait NodeData<L, S: ListState = (), SP: SyncPrimitives = DefaultSyncPrimitives>:
    Sized
{
    fn new_state_if_last_node_on_drop(self: Pin<&mut Self>, list: &L) -> S;
    fn on_drop<'list>(
        self: Pin<&mut Self>,
        list: &'list L,
        locked: Option<LockedList<'list, Self, S, SP>>,
        state_updated_on_unlink: bool,
    );
}

#[repr(align(4))]
pub(crate) struct NodeLink {
    pub(crate) prev: AtomicPtr<NodeLink>,
    pub(crate) next: AtomicPtr<NodeLink>,
}

impl NodeLink {
    #[cfg_attr(loom, const_fn::const_fn(cfg(false)))]
    pub(crate) const fn new() -> Self {
        Self {
            prev: AtomicPtr::new(NULL),
            next: AtomicPtr::new(NULL),
        }
    }

    #[inline(always)]
    pub(crate) fn next(&self) -> Option<NonNull<NodeLink>> {
        NonNull::new(self.next.load(Acquire))
    }

    #[inline(always)]
    pub(crate) fn is_linked(&self) -> bool {
        !self.prev.load(Acquire).is_null()
    }
}

#[repr(C)]
pub(crate) struct NodeInner<T> {
    pub(crate) link: NodeLink,
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
    L: AsList<List<T, S, SP>>,
    T: NodeData<L, S, SP>,
    S: ListState,
    SP: SyncPrimitives,
> {
    Unlinked(NodeUnlinked<'a, L, T, S, SP>),
    Linked(NodeLinked<'a, L, T, S, SP>),
}

pub struct Node<
    L: AsList<List<T, S, SP>>,
    T: NodeData<L, S, SP>,
    S: ListState = (),
    SP: SyncPrimitives = DefaultSyncPrimitives,
> {
    list: L,
    node: UnsafePinned<NodeInner<T>>,
    linked: Cell<bool>,
    _state: PhantomData<S>,
    _sync: PhantomData<SP>,
}

unsafe impl<
    L: AsList<List<T, S, SP>> + Send,
    T: NodeData<L, S, SP> + Send,
    S: ListState,
    SP: SyncPrimitives + Send,
> Send for Node<L, T, S, SP>
{
}
unsafe impl<
    L: AsList<List<T, S, SP>> + Sync,
    T: NodeData<L, S, SP>,
    S: ListState,
    SP: SyncPrimitives + Sync,
> Sync for Node<L, T, S, SP>
{
}

impl<L: AsList<List<T, S, SP>>, T: NodeData<L, S, SP>, S: ListState, SP: SyncPrimitives>
    Node<L, T, S, SP>
{
    pub fn new(list: L) -> Self
    where
        T: Default,
    {
        Self::with_data(list, Default::default())
    }

    #[cfg_attr(loom, const_fn::const_fn(cfg(false)))]
    pub const fn with_data(list: L, data: T) -> Self {
        Self {
            list,
            node: UnsafePinned::new(NodeInner {
                link: NodeLink::new(),
                data,
                #[cfg(loom)]
                access: loom::cell::Cell::new(()),
            }),
            linked: Cell::new(false),
            _state: PhantomData,
            _sync: PhantomData,
        }
    }

    #[inline(always)]
    pub const fn list(&self) -> &L {
        &self.list
    }

    fn link(&self) -> NonNull<NodeLink> {
        NonNull::new(self.node.get()).unwrap().cast()
    }

    #[inline(always)]
    pub fn is_linked(&self) -> bool {
        unsafe { (*self.node.get()).link.is_linked() }
    }

    #[inline(always)]
    pub fn state(self: Pin<&mut Self>) -> NodeState<'_, L, T, S, SP> {
        let this = self.into_ref().get_ref();
        if this.linked.get() {
            if this.is_linked() {
                let locked = this.list.as_list().lock();
                if this.is_linked() {
                    return NodeState::Linked(NodeLinked { node: this, locked });
                }
            }
            this.linked.set(false);
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

impl<L: AsList<List<T, S, SP>>, T: NodeData<L, S, SP>, S: ListState, SP: SyncPrimitives> Drop
    for Node<L, T, S, SP>
{
    #[inline]
    fn drop(&mut self) {
        if self.linked.get() && self.is_linked() {
            self.drop_linked();
        } else {
            (NodeDropped(self).data_mut()).on_drop(&self.list, None, false);
        }
    }
}

pub struct NodeUnlinked<
    'a,
    L: AsList<List<T, S, SP>>,
    T: NodeData<L, S, SP>,
    S: ListState = (),
    SP: SyncPrimitives = DefaultSyncPrimitives,
>(&'a Node<L, T, S, SP>);

unsafe impl<
    L: AsList<List<T, S, SP>> + Sync,
    T: NodeData<L, S, SP> + Send,
    S: ListState,
    SP: SyncPrimitives,
> Send for NodeUnlinked<'_, L, T, S, SP>
{
}
unsafe impl<
    L: AsList<List<T, S, SP>> + Sync,
    T: NodeData<L, S, SP> + Sync,
    S: ListState,
    SP: SyncPrimitives,
> Sync for NodeUnlinked<'_, L, T, S, SP>
{
}

node_ref!(
    NodeUnlinked<
        'a,
        L: AsList<List<T, S, SP>>,
        T: NodeData<L, S, SP>,
        S: ListState,
        SP: SyncPrimitives,
    >,
    T,
    self.0.link()
);

impl<'a, L: AsList<List<T, S, SP>>, T: NodeData<L, S, SP>, S: ListState, SP: SyncPrimitives>
    NodeUnlinked<'a, L, T, S, SP>
{
    #[inline]
    pub fn list(&self) -> &'a L {
        self.0.list()
    }
}

impl<'a, L: AsList<List<T, (), SP>>, T: NodeData<L, (), SP>, SP: SyncPrimitives>
    NodeUnlinked<'a, L, T, (), SP>
{
    #[inline]
    pub fn push_back(self, order: Ordering) {
        let list = self.list().as_list();
        let on_pushed = || self.0.linked.set(true);
        let link = self.0.link();
        let _ = unsafe { list.push_back(link, order, Relaxed, |_| None, |_| true, on_pushed) };
    }
}

impl<'a, L: AsList<List<T, usize, SP>>, T: NodeData<L, usize, SP>, SP: SyncPrimitives>
    NodeUnlinked<'a, L, T, usize, SP>
{
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
        mut on_push_back: P,
    ) -> Result<usize, bool> {
        let list = self.list().as_list();
        let link = self.0.link();
        let f = |state| f(Self(self.0).data_mut(), state);
        let on_push_back = |state| on_push_back(Self(self.0).data_mut(), state);
        let on_pushed = || self.0.linked.set(true);
        unsafe { list.push_back(link, set_order, fetch_order, f, on_push_back, on_pushed) }
            .inspect(|&state| on_state_updated(self.data_mut(), state))
    }
}

pub struct NodeLinked<
    'a,
    L: AsList<List<T, S, SP>>,
    T: NodeData<L, S, SP>,
    S: ListState = (),
    SP: SyncPrimitives = DefaultSyncPrimitives,
> {
    node: &'a Node<L, T, S, SP>,
    locked: LockedList<'a, T, S, SP>,
}

unsafe impl<
    'a,
    L: AsList<List<T, S, SP>> + Sync,
    T: NodeData<L, S, SP> + Send,
    S: ListState,
    SP: SyncPrimitives,
> Send for NodeLinked<'a, L, T, S, SP>
where
    LockedList<'a, T, S, SP>: Send,
{
}
unsafe impl<
    'a,
    L: AsList<List<T, S, SP>> + Sync,
    T: NodeData<L, S, SP> + Sync,
    S: ListState,
    SP: SyncPrimitives + Sync,
> Sync for NodeLinked<'a, L, T, S, SP>
where
    LockedList<'a, T, S, SP>: Sync,
{
}

node_ref!(
    NodeLinked<
        'a,
        L: AsList<List<T, S, SP>>,
        T: NodeData<L, S, SP>,
        S: ListState,
        SP: SyncPrimitives,
    >,
    T,
    self.node.link()
);

impl<'a, L: AsList<List<T, S, SP>>, T: NodeData<L, S, SP>, S: ListState, SP: SyncPrimitives>
    NodeLinked<'a, L, T, S, SP>
{
    #[inline]
    pub fn list(&self) -> &'a L {
        self.node.list()
    }
}

impl<'a, L: AsList<List<T, (), SP>>, T: NodeData<L, (), SP>, SP: SyncPrimitives>
    NodeLinked<'a, L, T, (), SP>
{
    #[inline]
    #[allow(clippy::type_complexity)]
    pub fn unlink(mut self) -> (NodeUnlinked<'a, L, T, (), SP>, LockedList<'a, T, (), SP>) {
        unsafe { self.locked.remove(self.node.link(), || (), false, false) };
        self.node.linked.set(false);
        (NodeUnlinked(self.node), self.locked)
    }
}

impl<'a, L: AsList<List<T, usize, SP>>, T: NodeData<L, usize, SP>, SP: SyncPrimitives>
    NodeLinked<'a, L, T, usize, SP>
{
    #[inline]
    #[allow(clippy::type_complexity)]
    pub fn unlink<F: FnOnce() -> usize>(
        mut self,
        new_state_if_last_node: F,
    ) -> (
        NodeUnlinked<'a, L, T, usize, SP>,
        LockedList<'a, T, usize, SP>,
        bool,
    ) {
        let (next, tail) = unsafe {
            self.locked
                .remove(self.node.link(), new_state_if_last_node, false, false)
        };
        let state_updated = next.is_none() && tail.is_none();
        self.node.linked.set(false);
        (NodeUnlinked(self.node), self.locked, state_updated)
    }
}

struct NodeDropped<
    'a,
    L: AsList<List<T, S, SP>>,
    T: NodeData<L, S, SP>,
    S: ListState,
    SP: SyncPrimitives,
>(&'a Node<L, T, S, SP>);

node_ref!(
    NodeDropped<
        'a,
        L: AsList<List<T, S, SP>>,
        T: NodeData<L, S, SP>,
        S: ListState,
        SP: SyncPrimitives,
    >,
    T,
    self.0.link()
);

pub(crate) mod private {
    use core::ptr::NonNull;

    use crate::node::{NodeInner, NodeLink};

    pub(crate) trait NodeRef {
        fn node(&self) -> NonNull<NodeLink>;

        #[inline(always)]
        fn data_ptr<T>(&self) -> *mut T {
            let inner = self.node().as_ptr().cast::<NodeInner<T>>();
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
    ($ty:ident<$($lf:lifetime,)* $($arg:ident $(:$bound:path)?),* $(,)?>, $data:ty, self.$($node_path:tt)*) => {
        impl<$($lf,)* $($arg $(:$bound)?),*> crate::node::private::NodeRef
            for $ty<$($lf,)* $($arg),*>
        {
            #[inline(always)]
            fn node(&self) -> core::ptr::NonNull<crate::node::NodeLink> {
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
