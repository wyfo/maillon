use core::{
    hint,
    marker::PhantomData,
    ptr,
    ptr::NonNull,
    sync::atomic::Ordering::{self, AcqRel, Acquire, Relaxed, Release, SeqCst},
};

use crate::{
    list::{DrainGetEnd, GetBack, GetFront, HEAD_MARKER},
    loom::{AtomicPtrExt, cell::Cell, sync::atomic::AtomicPtr},
    node::NodeLink,
    sync::parker::{DEFAULT_SPIN_BEFORE_PARK, DefaultParker, Parker},
    utils::OptionNonNullExt,
};

#[allow(private_bounds)]
pub trait Linking: PrivateLinking + Send + Sync + 'static {
    #[doc(hidden)]
    type PreferredDrainGetEnd: DrainGetEnd;
}

pub(crate) trait PrivateLinking: Sized {
    type NextPtr: 'static;
    type Parker: Send + Sync + 'static;
    #[cfg(not(loom))]
    const NEW_NEXT: Self::NextPtr;
    #[cfg(not(loom))]
    const NEW_PARKER: Self::Parker;
    fn new_next(ptr: Option<NonNull<NodeLink<Self>>>) -> Self::NextPtr;
    #[cfg(loom)]
    fn new_parker() -> Self::Parker;
    /// Whether [`get_next`](Self::get_next) dereferences its `tail` argument, so a caller
    /// passing a value it read `Relaxed` must acquire the tail first.
    ///
    /// `false` where node publication rides the `next` chain: `get_next` acquires
    /// `node.next` itself and the tail is not the synchronisation channel. `true` where
    /// publication rides the tail's release sequence and `get_next` walks `prev` backwards
    /// from `tail` — there, using an unacquired tail races the enqueuer's non-atomic write
    /// of its own `prev`.
    const NODES_ACCESS_REQUIRES_TAIL_ACQUIRE: bool;
    /// The ordering of `push_back`'s tail CAS: the caller's request raised to this
    /// variant's floor. The argument is a *minimum*, so a request stronger than the floor
    /// on another axis is honoured on top of it.
    fn push_back_set_order(set_order: Ordering) -> Ordering;
    fn store_next(
        prev_next: NonNull<Self::NextPtr>,
        node: NonNull<NodeLink<Self>>,
        parker: &Self::Parker,
    );
    fn load_next(next: &Self::NextPtr) -> Option<NonNull<NodeLink<Self>>>;
    /// Returns the successor of `node`, or the front of the list when `node` is `None`.
    ///
    /// `next` is the slot holding that successor — `node.next`, or `head_ptr` itself when
    /// `node` is `None`. `head_ptr` is the head slot of the list (or of the drain) being
    /// walked; a variant that materialises links lazily writes it when the walk reaches
    /// the front, since no `remove` will do it if the front is never unlinked.
    fn get_next(
        node: Option<NonNull<NodeLink<Self>>>,
        next: &Self::NextPtr,
        tail: NonNull<NodeLink<Self>>,
        parker: &Self::Parker,
    ) -> NonNull<NodeLink<Self>>;
    fn update_next(next: &Self::NextPtr, ptr: Option<NonNull<NodeLink<Self>>>);
    fn update_next_mut(next: &mut Self::NextPtr, ptr: Option<NonNull<NodeLink<Self>>>) {
        Self::update_next(next, ptr);
    }
    fn wait_next(next: &Self::NextPtr, parker: &Self::Parker) -> Option<NonNull<NodeLink<Self>>>;
    fn drain_get_head(sentinel: &mut NodeLink<Self>) -> Option<NonNull<NodeLink<Self>>>;
}

const PARKED_TAG: usize = 1;

#[derive(Debug)]
pub struct Eager<
    P: Parker = DefaultParker,
    const SPIN_BEFORE_PARK: usize = DEFAULT_SPIN_BEFORE_PARK,
>(PhantomData<P>);
impl<P: Parker, const SPIN_BEFORE_PARK: usize> PrivateLinking for Eager<P, SPIN_BEFORE_PARK> {
    type NextPtr = AtomicPtr<NodeLink<Self>>;
    type Parker = P;
    #[allow(clippy::declare_interior_mutable_const)]
    #[cfg(not(loom))]
    const NEW_NEXT: Self::NextPtr = AtomicPtr::new(ptr::null_mut());
    #[cfg(not(loom))]
    const NEW_PARKER: Self::Parker = P::INIT;
    fn new_next(ptr: Option<NonNull<NodeLink<Self>>>) -> Self::NextPtr {
        AtomicPtr::new(ptr.as_ptr())
    }
    #[cfg(loom)]
    fn new_parker() -> Self::Parker {
        P::new()
    }
    const NODES_ACCESS_REQUIRES_TAIL_ACQUIRE: bool = false;
    fn push_back_set_order(set_order: Ordering) -> Ordering {
        match set_order {
            Relaxed | Acquire | Release | AcqRel => AcqRel,
            _ => SeqCst, // `Ordering` is `#[non_exhaustive]`
        }
    }
    fn store_next(
        prev_next: NonNull<Self::NextPtr>,
        node: NonNull<NodeLink<Self>>,
        parker: &Self::Parker,
    ) {
        if P::NEVER_BLOCKS {
            unsafe { prev_next.as_ref() }.store(node.as_ptr(), Release);
        } else {
            let tagged_parked_state = unsafe { prev_next.as_ref().swap(node.as_ptr(), Release) };
            if !tagged_parked_state.is_null() {
                #[cold]
                #[inline(never)]
                fn unpark<P: Parker>(parker: &P, tagged_parked_state: *mut ()) {
                    unsafe {
                        parker.unpark(tagged_parked_state.map_addr(|addr| addr & !PARKED_TAG));
                    }
                }
                unpark(parker, tagged_parked_state.cast());
            }
        }
    }
    fn load_next(next: &Self::NextPtr) -> Option<NonNull<NodeLink<Self>>> {
        NonNull::new(next.load(Acquire))
    }
    fn get_next(
        _node: Option<NonNull<NodeLink<Self>>>,
        next: &Self::NextPtr,
        _tail: NonNull<NodeLink<Self>>,
        parker: &Self::Parker,
    ) -> NonNull<NodeLink<Self>> {
        if let Some(next) = Self::load_next(next) {
            return next;
        }
        #[cold]
        #[inline(never)]
        fn wait_for_next<P: Parker, const SPIN_BEFORE_PARK: usize>(
            next: &AtomicPtr<NodeLink<Eager<P, SPIN_BEFORE_PARK>>>,
            parker: &P,
        ) -> NonNull<NodeLink<Eager<P, SPIN_BEFORE_PARK>>> {
            if P::NEVER_BLOCKS {
                return unsafe { parker.park_until(|| NonNull::new(next.load(Acquire))) };
            }
            for _ in 0..SPIN_BEFORE_PARK {
                hint::spin_loop();
                if let Some(next) = NonNull::new(next.load(Acquire)) {
                    return next;
                }
            }
            let parked_state = parker.parked_state().map_addr(|addr| addr | PARKED_TAG);
            if let Err(next) =
                next.compare_exchange(ptr::null_mut(), parked_state.cast(), Relaxed, Acquire)
            {
                return unsafe { NonNull::new_unchecked(next) };
            }
            let load_next = || {
                let next = next.load(Acquire);
                (next.addr() & PARKED_TAG == 0).then(|| unsafe { NonNull::new_unchecked(next) })
            };
            unsafe { parker.park_until(load_next) }
        }
        wait_for_next::<P, SPIN_BEFORE_PARK>(next, parker)
    }
    fn update_next(next: &Self::NextPtr, ptr: Option<NonNull<NodeLink<Self>>>) {
        next.store(ptr.as_ptr(), Relaxed);
    }
    fn update_next_mut(next: &mut Self::NextPtr, ptr: Option<NonNull<NodeLink<Self>>>) {
        next.store_mut(ptr.as_ptr());
    }
    fn wait_next(next: &Self::NextPtr, parker: &Self::Parker) -> Option<NonNull<NodeLink<Self>>> {
        Some(Self::get_next(None, next, NonNull::dangling(), parker))
    }
    fn drain_get_head(sentinel: &mut NodeLink<Self>) -> Option<NonNull<NodeLink<Self>>> {
        NonNull::new(sentinel.next.load_mut())
    }
}
impl<P: Parker, const SPIN_BEFORE_PARK: usize> Linking for Eager<P, SPIN_BEFORE_PARK> {
    type PreferredDrainGetEnd = GetFront;
}

#[derive(Debug)]
pub struct Lazy;
impl PrivateLinking for Lazy {
    type NextPtr = Cell<Option<NonNull<NodeLink<Self>>>>;
    type Parker = ();
    #[allow(clippy::declare_interior_mutable_const)]
    #[cfg(not(loom))]
    const NEW_NEXT: Self::NextPtr = Cell::new(None);
    #[cfg(not(loom))]
    const NEW_PARKER: Self::Parker = ();
    fn new_next(ptr: Option<NonNull<NodeLink<Self>>>) -> Self::NextPtr {
        Cell::new(ptr)
    }
    #[cfg(loom)]
    fn new_parker() -> Self::Parker {}
    const NODES_ACCESS_REQUIRES_TAIL_ACQUIRE: bool = true;
    fn push_back_set_order(set_order: Ordering) -> Ordering {
        match set_order {
            Relaxed | Release => Release,
            Acquire | AcqRel => AcqRel,
            _ => SeqCst, // `Ordering` is `#[non_exhaustive]`
        }
    }
    fn store_next(
        _prev_next: NonNull<Self::NextPtr>,
        _node: NonNull<NodeLink<Self>>,
        _parker: &Self::Parker,
    ) {
    }
    fn load_next(next: &Self::NextPtr) -> Option<NonNull<NodeLink<Self>>> {
        next.get()
    }
    fn get_next(
        node: Option<NonNull<NodeLink<Self>>>,
        next: &Self::NextPtr,
        tail: NonNull<NodeLink<Self>>,
        _parker: &Self::Parker,
    ) -> NonNull<NodeLink<Self>> {
        debug_assert_ne!(node, Some(tail));
        if let Some(next) = Self::load_next(next) {
            return next;
        }
        #[cold]
        #[inline(never)]
        fn find_next(
            node: Option<NonNull<NodeLink<Lazy>>>,
            mut tail: NonNull<NodeLink<Lazy>>,
        ) -> NonNull<NodeLink<Lazy>> {
            loop {
                let prev = unsafe { tail.as_ref().load_prev() };
                // TODO not writing the next pointer of the last node is actually a good thing,
                // because it will surely be overwritten just after (when the node is removed)
                // and it prevents a segfault because prev can be HEAD_MARKER
                if Some(prev) == node {
                    return tail;
                } else if prev.addr().get() == HEAD_MARKER {
                    if node.is_none() {
                        return tail;
                    } else {
                        break;
                    }
                }
                unsafe { prev.as_ref().next.set(Some(tail)) }
                tail = prev;
            }
            // the node is in a drain cyclic chain
            let node = unsafe { node.unwrap_unchecked() };
            let mut prev = unsafe { node.as_ref().load_prev() };
            loop {
                let prev_prev = unsafe { prev.as_ref().load_prev() };
                if prev_prev == node {
                    return prev;
                }
                unsafe { prev_prev.as_ref().next.set(Some(prev)) }
                prev = prev_prev;
            }
        }
        let found = find_next(node, tail);
        if node.is_none() {
            // TODO If the node is None, the next pointer is assumed to be the head
            // it's better to materialized the head because the front might not be unlinked
            // so the head will be reused after
            next.set(Some(found));
        }
        found
    }
    fn update_next(next: &Self::NextPtr, ptr: Option<NonNull<NodeLink<Self>>>) {
        next.set(ptr);
    }
    fn wait_next(_next: &Self::NextPtr, _parker: &Self::Parker) -> Option<NonNull<NodeLink<Self>>> {
        None
    }
    fn drain_get_head(sentinel: &mut NodeLink<Self>) -> Option<NonNull<NodeLink<Self>>> {
        if let Some(head) = sentinel.next.get() {
            return Some(head);
        }
        let tail = NonNull::new(sentinel.prev.load(Relaxed))?;
        Some(Self::get_next(None, &sentinel.next, tail, &()))
    }
}
impl Linking for Lazy {
    type PreferredDrainGetEnd = GetBack;
}
