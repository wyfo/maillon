use core::{
    pin::{Pin, pin},
    ptr,
    ptr::NonNull,
};

use crate::{
    List,
    list::{GetBack, GetFront, IntoTail, ListState, LockedList, NodeLink, TailExt},
    loom::{
        AtomicPtrExt,
        sync::atomic::{AtomicPtr, Ordering::*},
    },
    node::{NULL, NodeRef, node_ref},
    sync::{DefaultSyncPrimitives, SyncPrimitives},
    utils::{OptionNonNullExt, defer},
};

pub struct Drain<'a, T, S: ListState = (), SP: SyncPrimitives + 'a = DefaultSyncPrimitives> {
    sentinel_node: NodeLink,
    queue: &'a List<T, S, SP>,
    locked: Option<LockedList<'a, T, S, SP>>,
}

impl<'a, T, S: ListState, SP: SyncPrimitives> Drain<'a, T, S, SP> {
    pub(super) fn new<F: FnOnce() -> S>(
        locked: LockedList<'a, T, S, SP>,
        new_state_if_not_empty: F,
    ) -> Self {
        let mut head = None;
        let mut tail = None;
        if locked.tail().is_some() {
            head = Some(locked.get_next(&locked.queue.head));
            locked.head.store(NULL, Relaxed);
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
                next: AtomicPtr::new(head.as_ptr()),
            },
            queue: locked.queue,
            locked: Some(locked),
        }
    }

    fn head(&mut self) -> Option<NonNull<NodeLink>> {
        NonNull::new(self.sentinel_node.next.load_mut())
    }

    fn tail(&mut self) -> Option<NonNull<NodeLink>> {
        NonNull::new(self.sentinel_node.prev.load_mut())
    }

    fn set_head(&mut self, head: Option<NonNull<NodeLink>>) {
        self.sentinel_node.next.store_mut(head.as_ptr());
    }

    fn set_tail(&mut self, tail: Option<NonNull<NodeLink>>) {
        self.sentinel_node.prev.store_mut(tail.as_ptr());
    }

    pub fn is_empty(&self) -> bool {
        self.sentinel_node.next.load(Relaxed).is_null()
    }

    fn check_locked(&self) {
        if self.locked.is_none() {
            #[inline(always)]
            fn panic_unlocked() -> ! {
                panic!("list lock should be held");
            }
            panic_unlocked();
        }
    }

    #[inline]
    pub fn front(self: Pin<&mut Self>) -> Option<DrainFront<'a, '_, T, S, SP>> {
        self.check_locked();
        let this = unsafe { self.get_unchecked_mut() };
        Some(DrainFront {
            node: this.head()?,
            drain: this,
        })
    }

    #[inline]
    pub fn back(self: Pin<&mut Self>) -> Option<DrainBack<'a, '_, T, S, SP>> {
        self.check_locked();
        let this = unsafe { self.get_unchecked_mut() };
        Some(DrainBack {
            node: this.tail()?,
            drain: this,
        })
    }

    pub fn execute_unlocked<F: FnOnce() -> R, R>(self: Pin<&mut Self>, f: F) -> R {
        self.check_locked();
        let this = unsafe { self.get_unchecked_mut() };
        // TODO constructing the pointer from a const ref should matter
        let sentinel_ptr = ptr::from_ref(&this.sentinel_node).cast_mut();
        if let Some(head) = this.head() {
            unsafe { head.as_ref().prev.store(sentinel_ptr, Relaxed) }
            let tail = unsafe { this.tail().unwrap_unchecked() };
            unsafe { tail.as_ref().next.store(sentinel_ptr, Relaxed) };
        }
        drop(unsafe { this.locked.take().unwrap_unchecked() });
        let _guard = defer(|| {
            this.locked = Some(this.queue.lock());
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
                    // Ends the borrow of `this`: borrowck must assume the projected end type
                    // has a destructor, so the borrow would otherwise live to the end of scope.
                    drop(end);
                    this.as_mut().execute_unlocked(|| on_unlock(helper));
                    end = E::get_end(this.as_mut());
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
        self.for_each_impl::<GetFront, _>(helper, on_next, on_unlock);
    }

    pub fn for_each_rev<H, N: FnMut(&mut H, Pin<&mut T>) -> bool, U: FnMut(&mut H)>(
        self,
        helper: &mut H,
        on_next: N,
        on_unlock: U,
    ) {
        self.for_each_impl::<GetBack, _>(helper, on_next, on_unlock);
    }

    #[cold]
    #[inline(never)]
    fn unlink_all(&mut self) {
        self.locked.get_or_insert_with(|| self.queue.lock());
        while let Some(node) = self.head()
            && node != (&self.sentinel_node).into()
        {
            DrainFront { node, drain: self }.unlink();
        }
    }
}

impl<'a, T, S: ListState, SP: SyncPrimitives> Drop for Drain<'a, T, S, SP> {
    #[inline]
    fn drop(&mut self) {
        if !self.sentinel_node.next.load(Relaxed).is_null() {
            self.unlink_all();
        }
    }
}

pub trait DrainEnd<'drain, 'a, T, S: ListState = (), SP: SyncPrimitives = DefaultSyncPrimitives>:
    NodeRef<T> + Sized
{
    fn unlink(self) -> Option<Self>;
}

pub struct DrainFront<'drain, 'a, T, S: ListState = (), SP: SyncPrimitives = DefaultSyncPrimitives>
{
    node: NonNull<NodeLink>,
    drain: &'a mut Drain<'drain, T, S, SP>,
}

unsafe impl<'drain, T: Send, S: ListState, SP: SyncPrimitives> Send
    for DrainFront<'drain, '_, T, S, SP>
where
    LockedList<'drain, T, S, SP>: Sync,
{
}
unsafe impl<'drain, T: Sync, S: ListState, SP: SyncPrimitives> Sync
    for DrainFront<'drain, '_, T, S, SP>
where
    LockedList<'drain, T, S, SP>: Sync,
{
}

impl<'drain, 'a, T, S: ListState, SP: SyncPrimitives> DrainEnd<'drain, 'a, T, S, SP>
    for DrainFront<'drain, 'a, T, S, SP>
{
    fn unlink(self) -> Option<Self> {
        DrainFront::unlink(self)
    }
}

impl<T, S: ListState, SP: SyncPrimitives> DrainFront<'_, '_, T, S, SP> {
    pub fn unlink(self) -> Option<Self> {
        let node = unsafe { self.node.as_ref() };
        let mut next = None;
        if self.drain.tail() != Some(self.node) {
            let locked = unsafe { self.drain.locked.as_mut().unwrap_unchecked() };
            next = Some(locked.get_next(&node.next));
        } else {
            self.drain.set_tail(None);
        }
        self.drain.set_head(next);
        node.prev.store(NULL, Release);
        Some(Self {
            node: next?,
            drain: self.drain,
        })
    }
}

node_ref!(
    DrainFront<'drain, 'a, T, S: ListState, SP: SyncPrimitives>,
    T,
    self.node
);

pub struct DrainBack<'drain, 'a, T, S: ListState = (), SP: SyncPrimitives = DefaultSyncPrimitives> {
    node: NonNull<NodeLink>,
    drain: &'a mut Drain<'drain, T, S, SP>,
}

unsafe impl<'drain, T: Send, S: ListState, SP: SyncPrimitives> Send
    for DrainBack<'drain, '_, T, S, SP>
where
    LockedList<'drain, T, S, SP>: Sync,
{
}
unsafe impl<'drain, T: Sync, S: ListState, SP: SyncPrimitives> Sync
    for DrainBack<'drain, '_, T, S, SP>
where
    LockedList<'drain, T, S, SP>: Sync,
{
}

impl<'drain, 'a, T, S: ListState, SP: SyncPrimitives> DrainEnd<'drain, 'a, T, S, SP>
    for DrainBack<'drain, 'a, T, S, SP>
{
    fn unlink(self) -> Option<Self> {
        DrainBack::unlink(self)
    }
}

impl<T, S: ListState, SP: SyncPrimitives> DrainBack<'_, '_, T, S, SP> {
    pub fn unlink(self) -> Option<Self> {
        let node = unsafe { self.node.as_ref() };
        let mut prev = None;
        if self.drain.head() != Some(self.node) {
            prev = NonNull::new(node.prev.load(Relaxed));
            let locked = unsafe { self.drain.locked.as_mut().unwrap_unchecked() };
            let next = locked.get_next(unsafe { &prev.unwrap().as_ref().next });
            debug_assert_eq!(next, node.into());
        } else {
            // TODO setting the head is required as the head is checked in many places
            self.drain.set_head(None);
        }
        self.drain.set_tail(prev);
        node.prev.store(NULL, Release);
        Some(Self {
            node: prev?,
            drain: self.drain,
        })
    }
}

node_ref!(
    DrainBack<'drain, 'a, T, S: ListState, SP: SyncPrimitives>,
    T,
    self.node
);

pub trait DrainGetEnd: Sized {
    type DrainEnd<'drain, 'a, T, S: ListState, SP: SyncPrimitives>: DrainEnd<'drain, 'a, T, S, SP>
    where
        'drain: 'a,
        T: 'drain;

    fn get_end<'drain, 'a, T, S: ListState, SP: SyncPrimitives>(
        drain: Pin<&'a mut Drain<'drain, T, S, SP>>,
    ) -> Option<Self::DrainEnd<'drain, 'a, T, S, SP>>;

    fn for_each<
        T,
        S: ListState,
        SP: SyncPrimitives,
        H,
        N: FnMut(&mut H, Pin<&mut T>) -> bool,
        U: FnMut(&mut H),
    >(
        drain: Drain<'_, T, S, SP>,
        helper: &mut H,
        on_next: N,
        on_unlock: U,
    ) {
        drain.for_each_impl::<Self, _>(helper, on_next, on_unlock);
    }
}

impl DrainGetEnd for GetFront {
    type DrainEnd<'drain, 'a, T, S: ListState, SP: SyncPrimitives>
        = DrainFront<'drain, 'a, T, S, SP>
    where
        'drain: 'a,
        T: 'drain;

    #[inline]
    fn get_end<'drain, 'a, T, S: ListState, SP: SyncPrimitives>(
        drain: Pin<&'a mut Drain<'drain, T, S, SP>>,
    ) -> Option<Self::DrainEnd<'drain, 'a, T, S, SP>> {
        drain.front()
    }
}

impl DrainGetEnd for GetBack {
    type DrainEnd<'drain, 'a, T, S: ListState, SP: SyncPrimitives>
        = DrainBack<'drain, 'a, T, S, SP>
    where
        'drain: 'a,
        T: 'drain;

    #[inline]
    fn get_end<'drain, 'a, T, S: ListState, SP: SyncPrimitives>(
        drain: Pin<&'a mut Drain<'drain, T, S, SP>>,
    ) -> Option<Self::DrainEnd<'drain, 'a, T, S, SP>> {
        drain.back()
    }
}
