use core::{
    mem::ManuallyDrop,
    pin::{Pin, pin},
    ptr,
    ptr::NonNull,
    task::Waker,
};

use crate::{
    list::{
        AtomicEager, GetBack, GetFront, HEAD_MARKER, IntoTail, Linking, ListState, LockedList,
        NodeLink, TailExt,
    },
    loom::{
        AtomicPtrExt,
        sync::atomic::{AtomicPtr, Ordering::*},
    },
    node::{LinkedNodeRef, node_ref},
    sync::mutex::{DefaultMutex, Mutex},
    utils::{OptionNonNullExt, defer},
    waker_batch::WakerBatch,
};

// TODO it should be possible to accept L: Linking, but it currently breaks everything with head
// always returning None
pub struct Drain<
    'a,
    T,
    S: ListState = (),
    D = (),
    L: Linking = AtomicEager,
    M: Mutex + 'a = DefaultMutex,
> {
    sentinel_node: NodeLink<L>,
    locked: ManuallyDrop<LockedList<'a, T, S, D, L, M>>,
}

impl<'a, T, S: ListState, D, L: Linking, M: Mutex> Drain<'a, T, S, D, L, M> {
    pub(super) fn new<F: FnOnce(&mut D) -> S>(
        mut locked: LockedList<'a, T, S, D, L, M>,
        new_state_if_not_empty: F,
    ) -> Self {
        let mut head = None;
        let mut tail = None;
        if locked.list.tail().is_some() {
            head = L::wait_next(&locked.list.head, &locked.list.parker);
            L::update_next(&locked.list.head, None);
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
            let new_tail = new_state_if_not_empty(locked.data_mut()).into_tail();
            let old_tail = if L::SERIALIZED {
                let tail = locked.list.tail.load(Relaxed);
                locked.list.tail.store(new_tail, Release);
                tail
            } else {
                locked.list.tail.swap(new_tail, AcqRel)
            };
            tail = Some(unsafe { old_tail.ptr().unwrap_unchecked() });
        }
        Self {
            sentinel_node: NodeLink {
                prev: AtomicPtr::new(tail.as_ptr()),
                next: L::new_next(head),
            },
            locked: ManuallyDrop::new(locked),
        }
    }

    fn head(&mut self) -> Option<NonNull<NodeLink<L>>> {
        L::drain_get_head(&mut self.sentinel_node)
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
    pub fn front(self: Pin<&mut Self>) -> Option<DrainFront<'a, '_, T, S, D, L, M>> {
        let this = unsafe { self.get_unchecked_mut() };
        Some(DrainFront {
            node: this.head()?,
            drain: this,
        })
    }

    #[inline]
    pub fn back(self: Pin<&mut Self>) -> Option<DrainBack<'a, '_, T, S, D, L, M>> {
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
        let list = unsafe { ManuallyDrop::take(&mut this.locked) }.unlock();
        let _guard = defer(|| {
            this.locked = ManuallyDrop::new(list.lock());
            if this.tail().as_ptr() == sentinel_ptr {
                debug_assert_eq!(this.head(), this.tail());
                this.set_head(None);
                this.set_tail(None);
            }
        });
        f()
    }

    #[inline]
    pub fn list_data(&self) -> &D {
        self.locked.data()
    }

    #[inline]
    pub fn list_data_mut(self: Pin<&mut Self>) -> &mut D {
        unsafe { self.get_unchecked_mut() }.locked.data_mut()
    }

    fn for_each_impl<E: DrainGetEnd, H>(
        self,
        helper: &mut H,
        mut on_next: impl FnMut(&mut H, Pin<&mut T>, &mut D) -> bool,
        mut on_unlock: impl FnMut(&mut H),
    ) {
        {
            let mut this = pin!(self);
            let mut end = E::get_end(this.as_mut());
            while let Some(mut node) = end {
                let (data, list_data) = node.split_data();
                let unlock = on_next(helper, data, list_data);
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

    fn wake_all_impl<E: DrainGetEnd, const WAKER_BATCH_SIZE: usize>(
        self,
        mut f: impl FnMut(Pin<&mut T>, &mut D) -> Option<Waker>,
    ) {
        self.for_each_impl::<E, _>(
            &mut WakerBatch::<WAKER_BATCH_SIZE>::new(),
            |wakers, node_data, list_data| {
                if let Some(waker) = f(node_data, list_data) {
                    wakers.push(waker);
                }
                wakers.is_full()
            },
            |wakers| wakers.wake_all(),
        );
    }

    pub fn for_each<H, N: FnMut(&mut H, Pin<&mut T>, &mut D) -> bool, U: FnMut(&mut H)>(
        self,
        helper: &mut H,
        on_next: N,
        on_unlock: U,
    ) {
        self.for_each_impl::<L::PreferredDrainGetEnd, _>(helper, on_next, on_unlock);
    }

    pub fn wake_all<
        const WAKER_BATCH_SIZE: usize,
        F: FnMut(Pin<&mut T>, &mut D) -> Option<Waker>,
    >(
        self,
        f: F,
    ) {
        self.wake_all_impl::<L::PreferredDrainGetEnd, WAKER_BATCH_SIZE>(f);
    }

    #[cold]
    #[inline(never)]
    fn unlink_all(&mut self) {
        let mut end = L::PreferredDrainGetEnd::get_end(unsafe { Pin::new_unchecked(self) });
        while let Some(node) = end {
            end = node.unlink();
        }
    }
}

impl<'a, T, S: ListState, D, L: Linking, M: Mutex> Drop for Drain<'a, T, S, D, L, M> {
    #[inline]
    fn drop(&mut self) {
        if !self.is_empty() {
            self.unlink_all();
        }
        unsafe { ManuallyDrop::drop(&mut self.locked) };
    }
}

pub trait DrainEnd<
    'drain,
    'a,
    T,
    S: ListState = (),
    D = (),
    L: Linking = AtomicEager,
    M: Mutex = DefaultMutex,
>: LinkedNodeRef<T, D> + Sized
{
    fn unlink(self) -> Option<Self>;
}

pub struct DrainFront<
    'drain,
    'a,
    T,
    S: ListState = (),
    D = (),
    L: Linking = AtomicEager,
    M: Mutex = DefaultMutex,
> {
    node: NonNull<NodeLink<L>>,
    drain: &'a mut Drain<'drain, T, S, D, L, M>,
}

unsafe impl<'drain, T: Send, S: ListState, D, L: Linking, M: Mutex> Send
    for DrainFront<'drain, '_, T, S, D, L, M>
where
    LockedList<'drain, T, S, D, L, M>: Sync,
{
}
unsafe impl<'drain, T: Sync, S: ListState, D, L: Linking, M: Mutex> Sync
    for DrainFront<'drain, '_, T, S, D, L, M>
where
    LockedList<'drain, T, S, D, L, M>: Sync,
{
}

impl<'drain, 'a, T, S: ListState, D, L: Linking, M: Mutex> DrainEnd<'drain, 'a, T, S, D, L, M>
    for DrainFront<'drain, 'a, T, S, D, L, M>
{
    fn unlink(self) -> Option<Self> {
        DrainFront::unlink(self)
    }
}

impl<T, S: ListState, D, L: Linking, M: Mutex> DrainFront<'_, '_, T, S, D, L, M> {
    pub fn unlink(self) -> Option<Self> {
        let node = unsafe { self.node.as_ref() };
        let mut next = None;
        // TODO there is at least one node so the tail cannot be null
        let tail = unsafe { self.drain.tail().unwrap_unchecked() };
        if tail != self.node {
            let locked = &self.drain.locked;
            next = Some(locked.get_next(Some(self.node), &node.next, tail));
        } else {
            self.drain.set_tail(None);
        }
        self.drain.set_head(next);
        node.unlink();
        Some(Self {
            node: next?,
            drain: self.drain,
        })
    }
}

node_ref!(
    DrainFront<'drain, 'a, T, S: ListState, D, L: Linking, M: Mutex>,
    (T, L, D),
    (self.node),
    (self.drain.locked)
);

pub struct DrainBack<
    'drain,
    'a,
    T,
    S: ListState = (),
    D = (),
    L: Linking = AtomicEager,
    M: Mutex = DefaultMutex,
> {
    node: NonNull<NodeLink<L>>,
    drain: &'a mut Drain<'drain, T, S, D, L, M>,
}

unsafe impl<'drain, T: Send, S: ListState, D, L: Linking, M: Mutex> Send
    for DrainBack<'drain, '_, T, S, D, L, M>
where
    LockedList<'drain, T, S, D, L, M>: Sync,
{
}
unsafe impl<'drain, T: Sync, S: ListState, D, L: Linking, M: Mutex> Sync
    for DrainBack<'drain, '_, T, S, D, L, M>
where
    LockedList<'drain, T, S, D, L, M>: Sync,
{
}

impl<'drain, 'a, T, S: ListState, D, L: Linking, M: Mutex> DrainEnd<'drain, 'a, T, S, D, L, M>
    for DrainBack<'drain, 'a, T, S, D, L, M>
{
    fn unlink(self) -> Option<Self> {
        DrainBack::unlink(self)
    }
}

impl<T, S: ListState, D, L: Linking, M: Mutex> DrainBack<'_, '_, T, S, D, L, M> {
    pub fn unlink(self) -> Option<Self> {
        let node = unsafe { self.node.as_ref() };
        let mut prev = Some(unsafe { node.load_prev() });
        if prev.as_ptr().addr() == HEAD_MARKER
            || prev.as_ptr() == ptr::from_mut(&mut self.drain.sentinel_node)
        {
            prev = None;
            self.drain.set_head(None);
        } else {
            let locked = &self.drain.locked;
            L::wait_next(unsafe { &prev.unwrap().as_ref().next }, &locked.list.parker);
        }
        self.drain.set_tail(prev);
        node.unlink();
        Some(Self {
            node: prev?,
            drain: self.drain,
        })
    }
}

node_ref!(
    DrainBack<'drain, 'a, T, S: ListState, D, L: Linking, M: Mutex>,
    (T, L, D),
    (self.node),
    (self.drain.locked)
);

pub trait DrainGetEnd: Sized {
    type DrainEnd<'drain, 'a, T, S: ListState, D, L: Linking, M: Mutex>: DrainEnd<'drain, 'a, T, S, D, L, M>
    where
        'drain: 'a,
        T: 'drain,
        D: 'drain;

    fn get_end<'drain, 'a, T, S: ListState, D, L: Linking, M: Mutex>(
        drain: Pin<&'a mut Drain<'drain, T, S, D, L, M>>,
    ) -> Option<Self::DrainEnd<'drain, 'a, T, S, D, L, M>>;

    fn for_each<
        T,
        S: ListState,
        D,
        L: Linking,
        M: Mutex,
        H,
        N: FnMut(&mut H, Pin<&mut T>, &mut D) -> bool,
        U: FnMut(&mut H),
    >(
        drain: Drain<'_, T, S, D, L, M>,
        helper: &mut H,
        on_next: N,
        on_unlock: U,
    ) {
        drain.for_each_impl::<Self, _>(helper, on_next, on_unlock);
    }

    fn wake_all<
        const WAKER_BATCH_SIZE: usize,
        T,
        S: ListState,
        D,
        L: Linking,
        M: Mutex,
        F: FnMut(Pin<&mut T>, &mut D) -> Option<Waker>,
    >(
        drain: Drain<'_, T, S, D, L, M>,
        f: F,
    ) {
        drain.wake_all_impl::<Self, WAKER_BATCH_SIZE>(f);
    }
}

impl DrainGetEnd for GetFront {
    type DrainEnd<'drain, 'a, T, S: ListState, D, L: Linking, M: Mutex>
        = DrainFront<'drain, 'a, T, S, D, L, M>
    where
        'drain: 'a,
        T: 'drain,
        D: 'drain;

    #[inline]
    fn get_end<'drain, 'a, T, S: ListState, D, L: Linking, M: Mutex>(
        drain: Pin<&'a mut Drain<'drain, T, S, D, L, M>>,
    ) -> Option<Self::DrainEnd<'drain, 'a, T, S, D, L, M>> {
        drain.front()
    }
}

impl DrainGetEnd for GetBack {
    type DrainEnd<'drain, 'a, T, S: ListState, D, L: Linking, M: Mutex>
        = DrainBack<'drain, 'a, T, S, D, L, M>
    where
        'drain: 'a,
        T: 'drain,
        D: 'drain;

    #[inline]
    fn get_end<'drain, 'a, T, S: ListState, D, L: Linking, M: Mutex>(
        drain: Pin<&'a mut Drain<'drain, T, S, D, L, M>>,
    ) -> Option<Self::DrainEnd<'drain, 'a, T, S, D, L, M>> {
        drain.back()
    }
}
