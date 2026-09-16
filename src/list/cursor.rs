use core::{pin::Pin, ptr::NonNull};

#[allow(unused_imports)]
use crate::msrv::StrictProvenance;
use crate::{
    list::{
        AtomicEager, HEAD_MARKER, Linking, ListRef, ListState, LockedList, NodeLink,
        PrivateLinking, Serialized,
    },
    loom::sync::atomic::Ordering,
    msrv::ptr,
    node::NodeUnlinked,
    sync::mutex::{DefaultMutex, Mutex},
};

pub struct ListCursor<
    'locked,
    'a,
    T,
    S: ListState = (),
    D = (),
    L: Linking = AtomicEager,
    M: Mutex = DefaultMutex,
> {
    node: Option<NonNull<NodeLink<L>>>,
    locked: &'a mut LockedList<'locked, T, S, D, L, M>,
}

unsafe impl<'locked, T: Send, S: ListState, D, L: Linking, M: Mutex> Send
    for ListCursor<'locked, '_, T, S, D, L, M>
where
    LockedList<'locked, T, S, D, L, M>: Send,
{
}
unsafe impl<'locked, T: Sync, S: ListState, D, L: Linking, M: Mutex> Sync
    for ListCursor<'locked, '_, T, S, D, L, M>
where
    LockedList<'locked, T, S, D, L, M>: Sync,
{
}

impl<'locked, 'a, T, S: ListState, D, L: Linking, M: Mutex> ListCursor<'locked, 'a, T, S, D, L, M> {
    #[inline]
    pub(super) fn new(
        node: Option<NonNull<NodeLink<L>>>,
        locked: &'a mut LockedList<'locked, T, S, D, L, M>,
    ) -> Self {
        Self { node, locked }
    }

    #[inline]
    pub fn current(&mut self) -> Option<Pin<&mut T>> {
        Some(unsafe { Pin::new_unchecked(&mut *NodeLink::data_ptr::<T>(self.node?)) })
    }

    #[inline]
    pub fn list_data(&self) -> &D {
        self.locked.data()
    }

    #[inline]
    pub fn list_data_mut(&mut self) -> &mut D {
        self.locked.data_mut()
    }

    #[inline]
    pub fn split_current_data(&mut self) -> Option<(Pin<&mut T>, &mut D)> {
        let current = unsafe { Pin::new_unchecked(&mut *NodeLink::data_ptr::<T>(self.node?)) };
        Some((current, self.locked.data_mut()))
    }

    #[inline]
    pub fn move_next(&mut self) {
        let next_ptr = (self.node).map_or(&self.locked.list.head, |n| unsafe { &n.as_ref().next });
        if let Some(next) = L::load_next(next_ptr) {
            self.node = Some(next);
            return;
        }
        let Some(tail) = self.locked.list.tail() else {
            // TODO the list is empty, so the cursor is already on the ghost node
            debug_assert!(self.node.is_none());
            return;
        };
        self.node = match self.node {
            Some(node) if node == tail => None,
            Some(node) => Some(self.locked.get_next(
                Some(node),
                unsafe { &node.as_ref().next },
                tail,
            )),
            None => Some(self.locked.get_next(None, &self.locked.list.head, tail)),
        };
    }

    #[inline]
    #[allow(clippy::incompatible_msrv, unstable_name_collisions)]
    pub fn move_prev(&mut self) {
        self.node = match self.node {
            // TODO no tail acquire needed here, the cursor position always comes from one
            Some(node) => {
                Some(unsafe { node.as_ref().load_prev() }).filter(|p| p.addr().get() != HEAD_MARKER)
            }
            None => self.locked.list.tail(),
        };
    }

    #[inline]
    fn remove_current_impl<F: FnOnce(Pin<&mut T>, &mut D) -> S>(
        &mut self,
        new_state_if_last_node: F,
    ) -> Option<bool> {
        let node = self.node?;
        let (next, tail) =
            unsafe { (self.locked).remove(node, new_state_if_last_node, false, false, true) };
        self.node = next;
        Some(next.is_none() && tail.is_none())
    }
}

impl<'locked, T, S: ListState, D, M: Mutex> ListCursor<'locked, '_, T, S, D, Serialized, M> {
    #[inline]
    pub fn insert_before<LR>(&mut self, node: NodeUnlinked<'_, LR>, order: Ordering)
    where
        LR: ListRef<NodeData = T, ListState = S, ListData = D, Linking = Serialized, Mutex = M>,
    {
        match self.node {
            Some(current) => {
                let prev = unsafe { current.as_ref().load_prev() };
                unsafe { self.locked.insert_between(node, prev, Some(current), order) };
            }
            None => self.locked.push_back(node, order),
        }
    }

    #[inline]
    pub fn insert_after<LR>(&mut self, node: NodeUnlinked<'_, LR>, order: Ordering)
    where
        LR: ListRef<NodeData = T, ListState = S, ListData = D, Linking = Serialized, Mutex = M>,
    {
        let (prev, next) = match self.node {
            Some(current) => (
                current,
                Serialized::load_next(unsafe { &current.as_ref().next }),
            ),
            None => (
                NonNull::new(ptr::without_provenance_mut(HEAD_MARKER)).unwrap(),
                Serialized::load_next(&self.locked.list.head),
            ),
        };
        unsafe { self.locked.insert_between(node, prev, next, order) };
    }
}

impl<T, D, L: Linking, M: Mutex> ListCursor<'_, '_, T, (), D, L, M> {
    #[inline]
    pub fn remove_current(&mut self) -> bool {
        self.remove_current_impl(|_, _| ()).is_some()
    }
}

impl<T, D, L: Linking, M: Mutex> ListCursor<'_, '_, T, usize, D, L, M> {
    #[inline]
    pub fn remove_current<F: FnOnce(Pin<&mut T>, &mut D) -> usize>(
        &mut self,
        new_state_if_last_node: F,
    ) -> Option<bool> {
        self.remove_current_impl(new_state_if_last_node)
    }
}
