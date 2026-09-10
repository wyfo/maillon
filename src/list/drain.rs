use core::{
    mem::ManuallyDrop,
    pin::{Pin, pin},
    ptr,
    ptr::NonNull,
};

use crate::{
    List,
    list::{Eager, GetBack, GetFront, IntoTail, Linking, ListState, LockedList, NodeLink, TailExt},
    loom::{
        AtomicPtrExt,
        sync::atomic::{AtomicPtr, Ordering::*},
    },
    node::{NodeRef, node_ref},
    sync::mutex::{DefaultMutex, Mutex},
    utils::{OptionNonNullExt, defer},
};

// TODO it should be possible to accept L: Linking, but it currently breaks everything with head
// always returning None
pub struct Drain<'a, T, S: ListState = (), L: Linking = Eager, M: Mutex + 'a = DefaultMutex> {
    sentinel_node: NodeLink<L>,
    queue: &'a List<T, S, L, M>,
    locked: ManuallyDrop<LockedList<'a, T, S, L, M>>,
}

impl<'a, T, S: ListState, L: Linking, M: Mutex> Drain<'a, T, S, L, M> {
    pub(super) fn new<F: FnOnce() -> S>(
        locked: LockedList<'a, T, S, L, M>,
        new_state_if_not_empty: F,
    ) -> Self {
        let mut head = None;
        let mut tail = None;
        if let Some(t) = locked.tail() {
            head = Some(locked.get_next(None, &locked.queue.head, t));
            L::update_next(&locked.head, None);
            // TODO
            // `Release` is for the `head` store just above: it must not sink past the swap.
            // Otherwise a concurrent enqueuer whose phase-1 CAS lands after the swap writes
            // its node into `head`, and the `null` store then clobbers that write. This is
            // worse than the equivalent race in `LockedList::remove`: there is no `Err` path
            // here to relink, so the lost node leaves a null `head` on a non-empty list and
            // the next dequeuer parks forever.
            //
            // No `Acquire` half is needed. The detached chain is walked through `Acquire`
            // `next` loads, which already cover every node in it, including the one this swap
            // returns.
            //
            // `SeqCst` is not needed either: the swap races with enqueue on the same atomic,
            // and coherence-ordered-before applies to any modification whatever its ordering
            // (https://github.com/rust-lang/miri/issues/5104, fixed by #5111), so the tail's
            // own modification order settles who wins.
            let new_tail = new_state_if_not_empty().into_tail();
            tail = Some(unsafe { locked.tail.swap(new_tail, AcqRel).ptr().unwrap_unchecked() });
        }
        Self {
            sentinel_node: NodeLink {
                prev: AtomicPtr::new(tail.as_ptr()),
                next: L::new_next(head),
            },
            queue: locked.queue,
            locked: ManuallyDrop::new(locked),
        }
    }

    fn head(&mut self) -> Option<NonNull<NodeLink<L>>> {
        L::load_next_mut(&mut self.sentinel_node.next)
    }

    fn tail(&mut self) -> Option<NonNull<NodeLink<L>>> {
        NonNull::new(self.sentinel_node.prev.load_mut())
    }

    fn set_head(&mut self, head: Option<NonNull<NodeLink<L>>>) {
        L::update_next_mut(&mut self.sentinel_node.next, head);
    }

    fn set_tail(&mut self, tail: Option<NonNull<NodeLink<L>>>) {
        self.sentinel_node.prev.store_mut(tail.as_ptr());
    }

    pub fn is_empty(&self) -> bool {
        self.sentinel_node.prev.load(Relaxed).is_null()
    }

    #[inline]
    pub fn front(self: Pin<&mut Self>) -> Option<DrainFront<'a, '_, T, S, L, M>> {
        let this = unsafe { self.get_unchecked_mut() };
        Some(DrainFront {
            node: this.head()?,
            drain: this,
        })
    }

    #[inline]
    pub fn back(self: Pin<&mut Self>) -> Option<DrainBack<'a, '_, T, S, L, M>> {
        let this = unsafe { self.get_unchecked_mut() };
        Some(DrainBack {
            node: this.tail()?,
            drain: this,
        })
    }

    pub fn execute_unlocked<F: FnOnce() -> R, R>(self: Pin<&mut Self>, f: F) -> R {
        let this = unsafe { self.get_unchecked_mut() };
        // TODO constructing the pointer from a const ref should matter
        let sentinel_ptr = ptr::from_ref(&this.sentinel_node).cast_mut();
        if let Some(head) = this.head() {
            unsafe { head.as_ref().prev.store(sentinel_ptr, Relaxed) }
            let tail = unsafe { this.tail().unwrap_unchecked() };
            unsafe { L::update_next(&tail.as_ref().next, NonNull::new(sentinel_ptr)) };
        }
        drop(unsafe { ManuallyDrop::take(&mut this.locked) });
        let _guard = defer(|| {
            this.locked = ManuallyDrop::new(this.queue.lock());
            if this.head().as_ptr() == sentinel_ptr {
                debug_assert_eq!(this.head(), this.tail());
                this.set_head(None);
                this.set_tail(None);
            }
        });
        f()
    }

    fn for_each_impl<E: DrainGetEnd, H>(
        self,
        helper: &mut H,
        mut on_next: impl FnMut(&mut H, Pin<&mut T>) -> bool,
        mut on_unlock: impl FnMut(&mut H),
    ) {
        {
            let mut this = pin!(self);
            let mut end = E::get_end(this.as_mut());
            while let Some(mut node) = end {
                let unlock = on_next(helper, node.data_mut());
                end = node.unlink();
                if unlock {
                    if end.is_none() {
                        break;
                    }
                    drop(end);
                    this.as_mut().execute_unlocked(|| on_unlock(helper));
                    end = E::get_end(this.as_mut());
                    if end.is_none() {
                        return;
                    }
                }
            }
        }
        on_unlock(helper);
    }

    pub fn for_each<H, N: FnMut(&mut H, Pin<&mut T>) -> bool, U: FnMut(&mut H)>(
        self,
        helper: &mut H,
        on_next: N,
        on_unlock: U,
    ) {
        self.for_each_impl::<L::PreferredDrainGetEnd, _>(helper, on_next, on_unlock);
    }

    #[cold]
    #[inline(never)]
    fn unlink_all(&mut self) {
        while let Some(node) = self.head()
            && node != (&self.sentinel_node).into()
        {
            DrainFront { node, drain: self }.unlink();
        }
    }
}

impl<'a, T, S: ListState, L: Linking, M: Mutex> Drop for Drain<'a, T, S, L, M> {
    #[inline]
    fn drop(&mut self) {
        if !self.is_empty() {
            self.unlink_all();
        }
        unsafe { ManuallyDrop::drop(&mut self.locked) };
    }
}

pub trait DrainEnd<'drain, 'a, T, S: ListState = (), L: Linking = Eager, M: Mutex = DefaultMutex>:
    NodeRef<T> + Sized
{
    fn unlink(self) -> Option<Self>;
}

pub struct DrainFront<'drain, 'a, T, S: ListState = (), L: Linking = Eager, M: Mutex = DefaultMutex>
{
    node: NonNull<NodeLink<L>>,
    drain: &'a mut Drain<'drain, T, S, L, M>,
}

unsafe impl<'drain, T: Send, S: ListState, L: Linking, M: Mutex> Send
    for DrainFront<'drain, '_, T, S, L, M>
where
    LockedList<'drain, T, S, L, M>: Sync,
{
}
unsafe impl<'drain, T: Sync, S: ListState, L: Linking, M: Mutex> Sync
    for DrainFront<'drain, '_, T, S, L, M>
where
    LockedList<'drain, T, S, L, M>: Sync,
{
}

impl<'drain, 'a, T, S: ListState, L: Linking, M: Mutex> DrainEnd<'drain, 'a, T, S, L, M>
    for DrainFront<'drain, 'a, T, S, L, M>
{
    fn unlink(self) -> Option<Self> {
        DrainFront::unlink(self)
    }
}

impl<T, S: ListState, L: Linking, M: Mutex> DrainFront<'_, '_, T, S, L, M> {
    pub fn unlink(self) -> Option<Self> {
        let node = unsafe { self.node.as_ref() };
        let mut next = None;
        // TODO there is at least one node so the tail cannot be null
        let tail = unsafe { self.drain.tail().unwrap_unchecked() };
        if tail != self.node {
            let locked = &self.drain.locked;
            next = Some(locked.get_next(Some(node.into()), &node.next, tail));
        } else {
            self.drain.set_tail(None);
        }
        self.drain.set_head(next);
        L::update_next(&node.next, None);
        node.prev.store(ptr::null_mut(), Release);
        Some(Self {
            node: next?,
            drain: self.drain,
        })
    }
}

node_ref!(
    DrainFront<'drain, 'a, T, S: ListState, L: Linking, M: Mutex>,
    T,
    L,
    self.node
);

pub struct DrainBack<'drain, 'a, T, S: ListState = (), L: Linking = Eager, M: Mutex = DefaultMutex>
{
    node: NonNull<NodeLink<L>>,
    drain: &'a mut Drain<'drain, T, S, L, M>,
}

unsafe impl<'drain, T: Send, S: ListState, L: Linking, M: Mutex> Send
    for DrainBack<'drain, '_, T, S, L, M>
where
    LockedList<'drain, T, S, L, M>: Sync,
{
}
unsafe impl<'drain, T: Sync, S: ListState, L: Linking, M: Mutex> Sync
    for DrainBack<'drain, '_, T, S, L, M>
where
    LockedList<'drain, T, S, L, M>: Sync,
{
}

impl<'drain, 'a, T, S: ListState, L: Linking, M: Mutex> DrainEnd<'drain, 'a, T, S, L, M>
    for DrainBack<'drain, 'a, T, S, L, M>
{
    fn unlink(self) -> Option<Self> {
        DrainBack::unlink(self)
    }
}

impl<T, S: ListState, L: Linking, M: Mutex> DrainBack<'_, '_, T, S, L, M> {
    pub fn unlink(self) -> Option<Self> {
        let node = unsafe { self.node.as_ref() };
        let mut prev = None;
        if self.drain.head() != Some(self.node) {
            prev = Some(unsafe { node.load_prev() });
            let locked = &self.drain.locked;
            L::wait_next(unsafe { &prev.unwrap().as_ref().next }, &locked.parker);
        } else {
            // TODO setting the head is required as the head is checked in many places
            self.drain.set_head(None);
        }
        self.drain.set_tail(prev);
        L::update_next(&node.next, None);
        node.prev.store(ptr::null_mut(), Release);
        Some(Self {
            node: prev?,
            drain: self.drain,
        })
    }
}

node_ref!(
    DrainBack<'drain, 'a, T, S: ListState, L: Linking, M: Mutex>,
    T,
    L,
    self.node
);

pub trait DrainGetEnd: Sized {
    type DrainEnd<'drain, 'a, T, S: ListState, L: Linking, M: Mutex>: DrainEnd<'drain, 'a, T, S, L, M>
    where
        'drain: 'a,
        T: 'drain;

    fn get_end<'drain, 'a, T, S: ListState, L: Linking, M: Mutex>(
        drain: Pin<&'a mut Drain<'drain, T, S, L, M>>,
    ) -> Option<Self::DrainEnd<'drain, 'a, T, S, L, M>>;

    fn for_each_from<
        T,
        S: ListState,
        L: Linking,
        M: Mutex,
        H,
        N: FnMut(&mut H, Pin<&mut T>) -> bool,
        U: FnMut(&mut H),
    >(
        drain: Drain<'_, T, S, L, M>,
        helper: &mut H,
        on_next: N,
        on_unlock: U,
    ) {
        drain.for_each_impl::<Self, _>(helper, on_next, on_unlock);
    }
}

impl DrainGetEnd for GetFront {
    type DrainEnd<'drain, 'a, T, S: ListState, L: Linking, M: Mutex>
        = DrainFront<'drain, 'a, T, S, L, M>
    where
        'drain: 'a,
        T: 'drain;

    #[inline]
    fn get_end<'drain, 'a, T, S: ListState, L: Linking, M: Mutex>(
        drain: Pin<&'a mut Drain<'drain, T, S, L, M>>,
    ) -> Option<Self::DrainEnd<'drain, 'a, T, S, L, M>> {
        drain.front()
    }
}

impl DrainGetEnd for GetBack {
    type DrainEnd<'drain, 'a, T, S: ListState, L: Linking, M: Mutex>
        = DrainBack<'drain, 'a, T, S, L, M>
    where
        'drain: 'a,
        T: 'drain;

    #[inline]
    fn get_end<'drain, 'a, T, S: ListState, L: Linking, M: Mutex>(
        drain: Pin<&'a mut Drain<'drain, T, S, L, M>>,
    ) -> Option<Self::DrainEnd<'drain, 'a, T, S, L, M>> {
        drain.back()
    }
}
