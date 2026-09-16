#[cfg(nightly)]
use core::pin::UnsafePinned;
use core::{marker::PhantomData, pin::Pin, ptr, ptr::NonNull};

#[allow(unused_imports)]
use crate::msrv::ResultExt;
#[cfg(not(nightly))]
use crate::unsafe_pinned::UnsafePinned;
use crate::{
    list::{Linking, ListRef},
    loom::{
        cell::Cell,
        sync::atomic::{AtomicPtr, Ordering, Ordering::*},
    },
};

#[allow(type_alias_bounds)]
type List<L: ListRef> =
    crate::list::List<L::NodeData, L::ListState, L::ListData, L::Linking, L::Mutex>;
#[allow(type_alias_bounds)]
type LockedList<'a, L: ListRef> =
    crate::list::LockedList<'a, L::NodeData, L::ListState, L::ListData, L::Linking, L::Mutex>;

pub trait NodeData<L: ListRef + ?Sized>: Sized {
    fn new_state_if_last_node_on_drop(
        self: Pin<&mut Self>,
        list: &L,
        list_data: &mut L::ListData,
    ) -> L::ListState;
    fn on_drop<'list>(
        self: Pin<&mut Self>,
        list: &'list L,
        locked: Option<LockedList<'list, L>>,
        state_updated_on_unlink: bool,
    );
}

mod private {
    use core::ptr::NonNull;

    use crate::{
        list::{Linking, PrivateLinking},
        loom::sync::atomic::AtomicPtr,
    };

    #[repr(align(4))]
    pub struct NodeLink<L: PrivateLinking> {
        pub(crate) prev: AtomicPtr<NodeLink<L>>,
        pub(crate) next: L::NextPtr,
    }

    pub trait PrivateNodeRef<T> {
        type Linking: Linking;

        fn link(&self) -> NonNull<NodeLink<Self::Linking>>;

        #[inline(always)]
        fn data_ptr(&self) -> *mut T {
            NodeLink::data_ptr(self.link())
        }
    }

    pub trait PrivateLinkedNodeRef<T, D>: PrivateNodeRef<T> {
        fn list_data_ptr(&self) -> *mut D;
    }
}
pub(crate) use private::{NodeLink, PrivateLinkedNodeRef, PrivateNodeRef};

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

    pub(crate) unsafe fn load_prev(&self) -> NonNull<NodeLink<L>> {
        unsafe { NonNull::new_unchecked(self.prev.load(Relaxed)) }
    }

    pub(crate) fn unlink(&self) {
        L::update_next(&self.next, None);
        self.prev.store(ptr::null_mut(), Release);
    }

    // TODO takes `NonNull<Self>`, not `&self`: a reference would only carry provenance over the
    // link, not over the whole `NodeInner`
    #[inline(always)]
    pub(crate) fn data_ptr<T>(link: NonNull<Self>) -> *mut T {
        let inner = link.as_ptr().cast::<NodeInner<T, L>>();
        #[cfg(loom)]
        unsafe {
            (*inner).access.set(());
        }
        unsafe { ptr::addr_of_mut!((*inner).data) }
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

pub enum NodeState<'a, L: ListRef> {
    Unlinked(NodeUnlinked<'a, L>),
    Linked(NodeLinked<'a, L>),
}

pub struct Node<L: ListRef> {
    list: L,
    node: UnsafePinned<NodeInner<L::NodeData, L::Linking>>,
    linked_list: Cell<Option<NonNull<List<L>>>>,
}

unsafe impl<L: ListRef + Send> Send for Node<L> where L::NodeData: Send {}
unsafe impl<L: ListRef + Sync> Sync for Node<L> {}

impl<L: ListRef> Node<L> {
    pub fn new(list: L) -> Self
    where
        L::NodeData: Default,
    {
        Self::with_data(list, Default::default())
    }

    #[cfg_attr(loom, const_fn::const_fn(cfg(false)))]
    pub const fn with_data(list: L, data: L::NodeData) -> Self {
        Self {
            list,
            node: UnsafePinned::new(NodeInner {
                link: NodeLink::new(),
                data,
                #[cfg(loom)]
                access: loom::cell::Cell::new(()),
            }),
            linked_list: Cell::new(None),
        }
    }

    #[inline(always)]
    pub const fn list(&self) -> &L {
        &self.list
    }

    fn link(&self) -> NonNull<NodeLink<L::Linking>> {
        NonNull::new(self.node.get()).unwrap().cast()
    }

    // TODO doc: false = never pushed, or already observed unlinked -> nothing set by the list
    // (notification etc.) can be pending; true = may be linked, or unlinked since by another
    // thread. Set by push_back, cleared by state()/unlink. Plain Cell read, no atomic, no lock.
    // Not the negation of is_linked, which is authoritative both ways.
    #[inline(always)]
    pub fn is_maybe_linked(&self) -> bool {
        self.linked_list.get().is_some()
    }

    #[inline(always)]
    fn linked_list(&self) -> Option<&List<L>> {
        Some(unsafe { self.linked_list.get()?.as_ref() })
    }

    #[inline(always)]
    pub fn is_linked(&self) -> bool {
        unsafe { !(*self.node.get()).link.prev.load(Acquire).is_null() }
    }

    #[inline(always)]
    pub fn state(self: Pin<&mut Self>) -> NodeState<'_, L> {
        let this = self.into_ref().get_ref();
        if let Some(list) = this.linked_list() {
            if this.is_linked() {
                let locked = list.lock();
                if this.is_linked() {
                    return NodeState::Linked(NodeLinked {
                        node: this,
                        locked,
                        _state: PhantomData,
                    });
                }
            }
            this.linked_list.set(None);
        }
        NodeState::Unlinked(NodeUnlinked(this))
    }

    #[inline]
    fn unlink<F: FnOnce(Pin<&mut L::NodeData>, &mut L::ListData) -> L::ListState>(
        &self,
        locked: &mut LockedList<'_, L>,
        new_state_if_last_node: F,
    ) -> bool {
        let (next, tail) =
            unsafe { locked.remove(self.link(), new_state_if_last_node, false, false, false) };
        next.is_none() && tail.is_none()
    }

    #[cold]
    #[inline(never)]
    fn drop_linked(&mut self) {
        let list = unsafe { self.linked_list().unwrap_unchecked() };
        let mut locked = list.lock();
        let mut state_updated = false;
        if self.is_linked() {
            state_updated = self.unlink(&mut locked, |data, list_data| {
                data.new_state_if_last_node_on_drop(&self.list, list_data)
            });
        }
        let data = unsafe { Pin::new_unchecked(&mut *NodeLink::data_ptr(self.link())) };
        L::NodeData::on_drop(data, &self.list, Some(locked), state_updated);
    }
}

impl<L: ListRef> Drop for Node<L> {
    #[inline]
    fn drop(&mut self) {
        if self.is_maybe_linked() && self.is_linked() {
            self.drop_linked();
        } else {
            let data = unsafe { Pin::new_unchecked(&mut *NodeLink::data_ptr(self.link())) };
            L::NodeData::on_drop(data, &self.list, None, false);
        }
    }
}

pub struct NodeUnlinked<'a, L: ListRef>(&'a Node<L>);

unsafe impl<L: ListRef + Sync> Send for NodeUnlinked<'_, L> where L::NodeData: Send {}
unsafe impl<L: ListRef + Sync> Sync for NodeUnlinked<'_, L> where L::NodeData: Sync {}

node_ref!(
    NodeUnlinked<'a, L: ListRef>,
    (L::NodeData, L::Linking),
    (self.0.link())
);

impl<'a, L: ListRef> NodeUnlinked<'a, L> {
    #[inline]
    pub fn list(&self) -> &'a L {
        self.0.list()
    }

    #[inline(always)]
    pub(crate) fn set_linked(&self, list: &List<L>) {
        (self.0.linked_list).set(Some(list.into()));
    }
}

impl<'a, L: ListRef<ListState = ()>> NodeUnlinked<'a, L> {
    #[inline]
    pub fn push_back(self, order: Ordering) {
        let list = self.list().as_list();
        let f = None::<fn(Pin<&mut L::NodeData>, ()) -> Option<()>>;
        let pushed = list.push_back(self, order, Relaxed, f, |_, _| true);
        debug_assert_eq!(pushed, Err(true));
    }
}

impl<'a, L: ListRef<ListState = usize>> NodeUnlinked<'a, L> {
    pub fn try_push_back_with<P: FnMut(Pin<&mut L::NodeData>, Option<usize>) -> bool>(
        self,
        set_order: Ordering,
        fetch_order: Ordering,
        on_push: P,
    ) -> bool {
        let list = self.list().as_list();
        let f = None::<fn(Pin<&mut L::NodeData>, usize) -> Option<usize>>;
        list.push_back(self, set_order, fetch_order, f, on_push)
            .unwrap_err()
    }

    #[allow(clippy::incompatible_msrv, unstable_name_collisions)]
    pub fn try_update_state_or_push_back_with<
        F: FnMut(Pin<&mut L::NodeData>, usize) -> Option<usize>,
        P: FnMut(Pin<&mut L::NodeData>, Option<usize>) -> bool,
        U: FnOnce(Pin<&mut L::NodeData>, usize),
    >(
        self,
        set_order: Ordering,
        fetch_order: Ordering,
        f: F,
        on_state_updated: U,
        on_push: P,
    ) -> Result<usize, bool> {
        let list = self.list().as_list();
        let mut this = Self(self.0);
        list.push_back(self, set_order, fetch_order, Some(f), on_push)
            .inspect(|&state| on_state_updated(this.data_mut(), state))
    }
}

pub struct NodeLinked<'a, L: ListRef, S = <L as ListRef>::ListState> {
    node: &'a Node<L>,
    locked: LockedList<'a, L>,
    _state: PhantomData<S>,
}

unsafe impl<'a, L: ListRef + Sync> Send for NodeLinked<'a, L>
where
    L::NodeData: Send,
    LockedList<'a, L>: Send,
{
}
#[allow(renamed_and_removed_lints, suspicious_auto_trait_impls)]
unsafe impl<'a, L: ListRef + Sync> Sync for NodeLinked<'a, L>
where
    L::NodeData: Sync,
    LockedList<'a, L>: Sync,
{
}

node_ref!(
    NodeLinked<'a, L: ListRef>,
    (L::NodeData, L::Linking, L::ListData),
    (self.node.link()),
    (self.locked)
);

impl<'a, L: ListRef> NodeLinked<'a, L> {
    #[inline]
    pub fn list(&self) -> &'a L {
        self.node.list()
    }
}

impl<'a, L: ListRef<ListState = ()>> NodeLinked<'a, L, ()> {
    #[inline]
    pub fn unlink(mut self) -> (NodeUnlinked<'a, L>, LockedList<'a, L>) {
        self.node.unlink(&mut self.locked, |_, _| ());
        self.node.linked_list.set(None);
        (NodeUnlinked(self.node), self.locked)
    }
}

impl<'a, L: ListRef<ListState = usize>> NodeLinked<'a, L, usize> {
    #[inline]
    pub fn unlink<F: FnOnce(Pin<&mut L::NodeData>, &mut L::ListData) -> L::ListState>(
        mut self,
        new_state_if_last_node: F,
    ) -> (NodeUnlinked<'a, L>, LockedList<'a, L>, bool) {
        let state_updated = self.node.unlink(&mut self.locked, new_state_if_last_node);
        self.node.linked_list.set(None);
        (NodeUnlinked(self.node), self.locked, state_updated)
    }
}

pub trait NodeRef<T>: PrivateNodeRef<T> {
    #[inline]
    fn data(&self) -> &T {
        unsafe { &*self.data_ptr() }
    }

    #[inline]
    fn data_mut(&mut self) -> Pin<&mut T> {
        unsafe { Pin::new_unchecked(&mut *self.data_ptr()) }
    }
}

pub trait LinkedNodeRef<T, D>: NodeRef<T> + PrivateLinkedNodeRef<T, D> {
    #[inline]
    fn list_data(&self) -> &D {
        unsafe { &*self.list_data_ptr() }
    }

    #[inline]
    fn list_data_mut(&mut self) -> &mut D {
        unsafe { &mut *self.list_data_ptr() }
    }

    #[inline]
    fn split_data(&mut self) -> (Pin<&mut T>, &mut D) {
        unsafe {
            (
                Pin::new_unchecked(&mut *self.data_ptr()),
                &mut *self.list_data_ptr(),
            )
        }
    }
}

macro_rules! node_ref {
    (
        $ty:ident<$($lf:lifetime,)* $($arg:ident $(:$bound:path)?),* $(,)?>,
        ($data:ty, $linking:ty, $list_data:ty),
        (self.$($node_path:tt)*),
        (self.$($locked_path:tt)*)
    ) => {
        crate::node::node_ref!(
            $ty<$($lf,)* $($arg $(:$bound)?),*>,
            ($data, $linking),
            (self.$($node_path)*)
        );

        impl<$($lf,)* $($arg $(:$bound)?),*> crate::node::PrivateLinkedNodeRef<$data, $list_data>
            for $ty<$($lf,)* $($arg),*>
        {
            #[inline(always)]
            fn list_data_ptr(&self) -> *mut $list_data {
                self.$($locked_path)*.data_ptr()
            }
        }

        impl<$($lf,)* $($arg $(:$bound)?),*> crate::node::LinkedNodeRef<$data, $list_data>
            for $ty<$($lf,)* $($arg),*>
        {
        }
    };
    (
        $ty:ident<$($lf:lifetime,)* $($arg:ident $(:$bound:path)?),* $(,)?>,
        ($data:ty, $linking:ty),
        (self.$($node_path:tt)*)
    ) => {
        impl<$($lf,)* $($arg $(:$bound)?),*> crate::node::PrivateNodeRef<$data>
            for $ty<$($lf,)* $($arg),*>
        {
            type Linking = $linking;

            #[inline(always)]
            fn link(&self) -> core::ptr::NonNull<crate::node::NodeLink<$linking>> {
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
