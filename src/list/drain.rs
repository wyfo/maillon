//! The [`Drain`] and its ends.
#[cfg(nightly)]
use core::pin::UnsafePinned;
use core::{marker::PhantomData, mem::ManuallyDrop, pin::Pin, ptr::NonNull, task::Waker};

#[allow(unused_imports)]
use crate::msrv::StrictProvenance;
#[cfg(not(nightly))]
use crate::unsafe_pinned::UnsafePinned;
use crate::{
    list::{
        AtomicEager, Back, End, Front, HEAD_MARKER, IntoTail, Linking, ListState, LockedList,
        NodeLink, TailExt,
    },
    loom::{
        AtomicPtrExt,
        sync::atomic::{AtomicPtr, Ordering::*},
    },
    msrv::ptr,
    node::{LinkedNodeRef, node_ref},
    sync::mutex::{DefaultMutex, Mutex},
    utils::{OptionNonNullExt, defer},
    waker_batch::WakerBatch,
};

/// A detached chain of all the nodes of a list, obtained from
/// [`LockedList::drain`].
///
/// Draining is atomic: the whole chain is detached from the list at once, so nodes pushed while
/// the drain is alive belong to the list, not to the drain. The list stays locked, but the lock
/// can be temporarily released with [`execute_unlocked`](Self::execute_unlocked).
///
/// Remaining nodes are unlinked when the drain is dropped.
pub struct Drain<
    'a,
    T,
    S: ListState = (),
    D = (),
    L: Linking = AtomicEager,
    M: Mutex + 'a = DefaultMutex,
> {
    sentinel_node: UnsafePinned<NodeLink<L>>,
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
            let new_tail = new_state_if_not_empty(locked.data_mut()).into_tail();
            let old_tail = if L::SERIALIZED {
                let tail = locked.list.tail.load(Relaxed);
                locked.list.tail.store(new_tail, Release);
                tail
            } else {
                // TODO Release ordering for the head store above
                // TODO Acquire ordering to synchronize with node insertion
                locked.list.tail.swap(new_tail, AcqRel)
            };
            tail = Some(unsafe { old_tail.ptr().unwrap_unchecked() });
        }
        Self {
            sentinel_node: UnsafePinned::new(NodeLink {
                prev: AtomicPtr::new(tail.as_ptr()),
                next: L::new_next(head),
            }),
            locked: ManuallyDrop::new(locked),
        }
    }

    fn sentinel(&mut self) -> &mut NodeLink<L> {
        unsafe { &mut *self.sentinel_node.get() }
    }

    fn head(&mut self) -> Option<NonNull<NodeLink<L>>> {
        L::drain_get_head(self.sentinel())
    }

    fn tail(&mut self) -> Option<NonNull<NodeLink<L>>> {
        NonNull::new(self.sentinel().prev.load_mut())
    }

    fn set_head(&mut self, head: Option<NonNull<NodeLink<L>>>) {
        L::update_next_mut(&mut self.sentinel().next, head);
    }

    fn set_tail(&mut self, tail: Option<NonNull<NodeLink<L>>>) {
        self.sentinel().prev.store_mut(tail.as_ptr());
    }

    /// Returns `true` if all the nodes of the drain have been unlinked.
    pub fn is_empty(&self) -> bool {
        let sentinel = unsafe { &*self.sentinel_node.get() };
        sentinel.prev.load(Relaxed).is_null()
    }

    /// Returns the node at the end `E` of the drain, `None` if it is empty.
    #[inline]
    pub fn end<E: End>(self: Pin<&mut Self>) -> Option<DrainEnd<'a, '_, E, T, S, D, L, M>> {
        let this = unsafe { self.get_unchecked_mut() };
        Some(DrainEnd {
            node: if E::IS_FRONT {
                this.head()?
            } else {
                this.tail()?
            },
            drain: this,
            _end: PhantomData,
        })
    }

    /// Returns the front node of the drain, `None` if it is empty.
    #[inline]
    pub fn front(self: Pin<&mut Self>) -> Option<DrainFront<'a, '_, T, S, D, L, M>> {
        self.end()
    }

    /// Returns the back node of the drain, `None` if it is empty.
    #[inline]
    pub fn back(self: Pin<&mut Self>) -> Option<DrainBack<'a, '_, T, S, D, L, M>> {
        self.end()
    }

    /// Executes `f` with the list unlocked, the lock being reacquired before returning.
    pub fn execute_unlocked<F: FnOnce() -> R, R>(self: Pin<&mut Self>, f: F) -> R {
        let this = unsafe { self.get_unchecked_mut() };
        if let Some(head) = this.head() {
            let tail = unsafe { this.tail().unwrap_unchecked() };
            let sentinel_ptr = ptr::from_mut(this.sentinel());
            unsafe { head.as_ref().prev.store(sentinel_ptr, Relaxed) }
            unsafe { L::update_next(&tail.as_ref().next, NonNull::new(sentinel_ptr)) };
        }
        let list = unsafe { ManuallyDrop::take(&mut this.locked) }.unlock();
        let _guard = defer(|| {
            this.locked = ManuallyDrop::new(list.lock());
            if ptr::eq(this.tail().as_ptr(), this.sentinel()) {
                debug_assert_eq!(this.head(), this.tail());
                this.set_head(None);
                this.set_tail(None);
            }
        });
        f()
    }

    /// Returns a reference to the list data.
    #[inline]
    pub fn list_data(&self) -> &D {
        self.locked.data()
    }

    /// Returns a mutable reference to the list data.
    #[inline]
    pub fn list_data_mut(self: Pin<&mut Self>) -> &mut D {
        unsafe { self.get_unchecked_mut() }.locked.data_mut()
    }

    fn for_each_impl<E: End, H>(
        self,
        mut helper: H,
        mut on_next: impl FnMut(&mut H, Pin<&mut T>, &mut D) -> bool,
        mut on_unlock: impl FnMut(&mut H),
    ) -> usize {
        let mut count = 0;
        {
            let mut moved_self = self;
            let mut this = unsafe { Pin::new_unchecked(&mut moved_self) };
            let mut end = this.as_mut().end::<E>();
            while let Some(mut node) = end {
                let (data, list_data) = node.split_data();
                let unlock = on_next(&mut helper, data, list_data);
                end = node.unlink();
                count += 1;
                if unlock {
                    if end.is_none() {
                        break;
                    }
                    this.as_mut().execute_unlocked(|| on_unlock(&mut helper));
                    end = this.as_mut().end::<E>();
                    if end.is_none() {
                        return count;
                    }
                }
            }
        }
        on_unlock(&mut helper);
        count
    }

    fn wake_all_impl<E: End, const WAKER_BATCH_SIZE: usize>(
        self,
        mut f: impl FnMut(Pin<&mut T>, &mut D) -> Option<Waker>,
    ) -> usize {
        self.for_each_impl::<E, _>(
            WakerBatch::<WAKER_BATCH_SIZE>::new(),
            |wakers, node_data, list_data| {
                if let Some(waker) = f(node_data, list_data) {
                    wakers.push(waker);
                }
                wakers.is_full()
            },
            |wakers| wakers.wake_all(),
        )
    }

    /// Unlinks all the nodes after calling `on_next` on them, and returns the number of unlinked
    /// nodes.
    ///
    /// If `on_next` returns `true` or once the drain is empty, then `on_unlock` is called with the
    /// lock released. A `helper` can be provided to be passed to both closures, allowing them to
    /// share a state, e.g. a [`WakerBatch`].
    pub fn for_each<H, N: FnMut(&mut H, Pin<&mut T>, &mut D) -> bool, U: FnMut(&mut H)>(
        self,
        helper: H,
        on_next: N,
        on_unlock: U,
    ) -> usize {
        self.for_each_impl::<L::PreferredDrainEnd, _>(helper, on_next, on_unlock)
    }

    /// Unlinks all the nodes of the chain, waking the wakers returned by `f`, and returns the
    /// number of unlinked nodes.
    ///
    /// Wakers are accumulated into a [`WakerBatch`] of `WAKER_BATCH_SIZE`, and woken with the lock
    /// released.
    pub fn wake_all<
        const WAKER_BATCH_SIZE: usize,
        F: FnMut(Pin<&mut T>, &mut D) -> Option<Waker>,
    >(
        self,
        f: F,
    ) -> usize {
        self.wake_all_impl::<L::PreferredDrainEnd, WAKER_BATCH_SIZE>(f)
    }

    #[cold]
    #[inline(never)]
    fn unlink_all(&mut self) {
        let mut end = unsafe { Pin::new_unchecked(self) }.end::<L::PreferredDrainEnd>();
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

/// An end of a [`Drain`] chain, either its [`DrainFront`] or its [`DrainBack`].
pub struct DrainEnd<
    'drain,
    'a,
    E: End,
    T,
    S: ListState = (),
    D = (),
    L: Linking = AtomicEager,
    M: Mutex = DefaultMutex,
> {
    node: NonNull<NodeLink<L>>,
    drain: &'a mut Drain<'drain, T, S, D, L, M>,
    _end: PhantomData<E>,
}

/// The front node of a [`Drain`] chain, obtained from [`Drain::front`].
pub type DrainFront<'drain, 'a, T, S = (), D = (), L = AtomicEager, M = DefaultMutex> =
    DrainEnd<'drain, 'a, Front, T, S, D, L, M>;

/// The back node of a [`Drain`] chain, obtained from [`Drain::back`].
pub type DrainBack<'drain, 'a, T, S = (), D = (), L = AtomicEager, M = DefaultMutex> =
    DrainEnd<'drain, 'a, Back, T, S, D, L, M>;

unsafe impl<'drain, E: End, T: Send, S: ListState, D, L: Linking, M: Mutex> Send
    for DrainEnd<'drain, '_, E, T, S, D, L, M>
where
    LockedList<'drain, T, S, D, L, M>: Sync,
{
}
unsafe impl<'drain, E: End, T: Sync, S: ListState, D, L: Linking, M: Mutex> Sync
    for DrainEnd<'drain, '_, E, T, S, D, L, M>
where
    LockedList<'drain, T, S, D, L, M>: Sync,
{
}

impl<E: End, T, S: ListState, D, L: Linking, M: Mutex> DrainEnd<'_, '_, E, T, S, D, L, M> {
    /// Unlinks the node, returning the new end of the drain, `None` if it becomes empty.
    #[allow(clippy::incompatible_msrv, unstable_name_collisions)]
    pub fn unlink(self) -> Option<Self> {
        let node = unsafe { self.node.as_ref() };
        let new_end = if E::IS_FRONT {
            let mut next = None;
            // SAFETY: there is at least one node so the tail cannot be null
            let tail = unsafe { self.drain.tail().unwrap_unchecked() };
            if tail != self.node {
                let locked = &self.drain.locked;
                next = Some(locked.get_next(Some(self.node), &node.next, tail));
            } else {
                self.drain.set_tail(None);
            }
            self.drain.set_head(next);
            next
        } else {
            let mut prev = Some(unsafe { node.load_prev() });
            if prev.as_ptr().addr() == HEAD_MARKER
                || prev.as_ptr() == ptr::from_mut(self.drain.sentinel())
            {
                prev = None;
                self.drain.set_head(None);
            } else {
                let locked = &self.drain.locked;
                L::wait_next(unsafe { &prev.unwrap().as_ref().next }, &locked.list.parker);
            }
            self.drain.set_tail(prev);
            prev
        };
        node.unlink();
        Some(Self {
            node: new_end?,
            drain: self.drain,
            _end: PhantomData,
        })
    }
}

node_ref!(
    DrainEnd<'drain, 'a, E: End, T, S: ListState, D, L: Linking, M: Mutex>,
    (T, L, D),
    (self.node),
    (self.drain.locked)
);
