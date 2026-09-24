use core::{
    marker::PhantomData,
    ptr::NonNull,
    sync::atomic::Ordering::{self, AcqRel, Acquire, Relaxed, Release, SeqCst},
};

#[allow(unused_imports)]
use crate::msrv::StrictProvenance;
use crate::{
    backoff::{BackoffLimit, BackoffStrategy, BoundedBackoffStrategy, NoBackoff, SpinBackoff},
    list::{Back, End, Front, HEAD_MARKER},
    loom::{AtomicPtrExt, cell::Cell, sync::atomic::AtomicPtr},
    msrv::ptr,
    node::NodeLink,
    sync::parker::{DefaultParker, Parker},
    utils::{OptionNonNullExt, abort_on_unwind},
};

/// How the nodes of a [`List`](crate::List) are linked together when a node is pushed to the back.
///
/// With atomic linking, i.e. [`AtomicEager`] and [`AtomicLazy`], node push to the back of the
/// list is lock-free[^1], as well as list state updates. On the other hand, [`Serialized`] linking
/// requires holding the list mutex to insert nodes or update the list state.
///
/// See each variant documentation for more details about their implications.
///
/// # Which variant to choose
///
/// The default `AtomicEager` should perform well in most situations.
///
/// `AtomicLazy` makes the node insertion a lot cheaper, and draining the list has no additional
/// cost. However, node removal can have high latency if the list contains a lot of nodes. For a
/// small number of nodes, or for drain-only workflows with few nodes dropped while linked, it
/// should be the more performant linking.
///
/// `Serialized` is mandatory to allow node insertion somewhere other than at the back of the list.
/// It is also possible that node insertion must access the list's data, and thus requires
/// serializing with the list mutex. Moreover, removing the back node, e.g. in LIFO workflows, is
/// costlier with atomic linking compared to `Serialized`.
///
/// Another rare issue with `AtomicEager` is priority inversion, when the pusher thread is
/// descheduled before unblocking a remover thread. However, this issue also exists (with a higher
/// probability) with mutexes that don't support priority inheritance, which is the case for
/// `std::sync::Mutex` on Linux or Windows. If priority inversion is a problem, then the provided
/// mutex should support priority inheritance and `AtomicLazy`/`Serialized` should be used instead.
///
/// In any case, profiling and benchmarking the different variants will often give the best answer.
///
/// [^1]: At least on mainstream platforms for `AtomicEager`.
pub trait Linking: PrivateLinking + Send + Sync + 'static {
    #[doc(hidden)]
    type PreferredDrainEnd: End;
}

mod private {
    use core::{ptr::NonNull, sync::atomic::Ordering};

    use crate::{backoff::BackoffStrategy, node::NodeLink};

    pub trait PrivateLinking: Sized {
        type NextPtr: 'static;
        type Parker: Send + Sync + 'static;
        type Backoff: BackoffStrategy;
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
        const SERIALIZED: bool;
        /// The ordering of `push_back`'s tail CAS: the caller's request raised to this
        /// variant's floor. The argument is a *minimum*, so a request stronger than the floor
        /// on another axis is honoured on top of it.
        fn push_back_set_order(set_order: Ordering) -> Ordering;
        fn store_next(
            prev: *mut NodeLink<Self>,
            head: &Self::NextPtr,
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
        fn wait_next(
            next: &Self::NextPtr,
            parker: &Self::Parker,
        ) -> Option<NonNull<NodeLink<Self>>>;
        fn drain_get_head(sentinel: &mut NodeLink<Self>) -> Option<NonNull<NodeLink<Self>>>;
    }
}
pub(crate) use private::PrivateLinking;

/// The default backoff strategy of [`AtomicEager`] before parking.
#[cfg(not(any(miri, loom)))]
pub type DefaultSpinBeforePark = BackoffLimit<SpinBackoff, 100>; // same as `std::sys::sync::mutex::futex`
/// The default backoff strategy of [`AtomicEager`] before parking.
#[cfg(any(miri, loom))]
pub type DefaultSpinBeforePark = BackoffLimit<SpinBackoff, 0>;

const PARKED_TAG: usize = 1;

/// Nodes are pushed atomically to the back of the list and link themselves to the previous node
/// eagerly.
///
/// A thread walking the list, e.g. to unlink its front node, may have to wait for a concurrent push
/// to link its node. Waiting is synchronized using the [`Parker`] `P`, and is preceded with a spin
/// loop bounded by `PB`.
///
/// On the platforms supported by the default `AtomicParker`, node push is lock-free. Otherwise, as
/// the pusher thread might unpark a remover thread, the lock-freedom is bounded by the unparking
/// operation.
///
/// Node push uses a CAS loop on the list state, followed by a second atomic RMW. Removal of the
/// back node, or list drain, requires a single RMW on the list state (in addition to the mutex
/// locking and unlocking). With a [`SpinParker`](crate::sync::parker::SpinParker) that
/// [never blocks](Parker::NEVER_BLOCKS), the second atomic RMW on push is replaced by an atomic
/// store.
///
/// As parking is necessary for soundness, unwinding in `Parker` methods causes the process to
/// abort.
///
/// `B` is the backoff strategy used on contention when pushing nodes or updating the list state.
#[derive(Debug)]
pub struct AtomicEager<
    B: BackoffStrategy = NoBackoff,
    P: Parker = DefaultParker,
    PB: BoundedBackoffStrategy = DefaultSpinBeforePark,
>(PhantomData<(B, P, PB)>);
impl<B: BackoffStrategy, P: Parker, PB: BoundedBackoffStrategy> PrivateLinking
    for AtomicEager<B, P, PB>
{
    type NextPtr = AtomicPtr<NodeLink<Self>>;
    type Parker = P;
    type Backoff = B;
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
    const SERIALIZED: bool = false;
    fn push_back_set_order(set_order: Ordering) -> Ordering {
        match set_order {
            Relaxed | Acquire | Release | AcqRel => AcqRel,
            _ => SeqCst, // `Ordering` is `#[non_exhaustive]`
        }
    }
    #[allow(clippy::incompatible_msrv, unstable_name_collisions)]
    fn store_next(
        prev: *mut NodeLink<Self>,
        head: &Self::NextPtr,
        node: NonNull<NodeLink<Self>>,
        parker: &Self::Parker,
    ) {
        let prev_next = if prev.addr() == HEAD_MARKER {
            head
        } else {
            unsafe { &(*prev).next }
        };
        if P::NEVER_BLOCKS {
            prev_next.store(node.as_ptr(), Release);
        } else {
            let tagged_parked_state = prev_next.swap(node.as_ptr(), Release);
            if !tagged_parked_state.is_null() {
                #[cold]
                #[inline(never)]
                #[allow(clippy::incompatible_msrv, unstable_name_collisions)]
                fn unpark<P: Parker>(parker: &P, tagged_parked_state: *mut ()) {
                    // TODO parker must not unwind, as node.linked_list would not bet set otherwise
                    // so the node would not be removed in drop.
                    abort_on_unwind(|| unsafe {
                        parker.unpark(tagged_parked_state.map_addr(|addr| addr & !PARKED_TAG));
                    });
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
        #[allow(clippy::incompatible_msrv, unstable_name_collisions)]
        fn wait_for_next<L: PrivateLinking, P: Parker, PB: BoundedBackoffStrategy>(
            next: &AtomicPtr<NodeLink<L>>,
            parker: &P,
        ) -> NonNull<NodeLink<L>> {
            if P::NEVER_BLOCKS {
                return abort_on_unwind(|| unsafe {
                    parker.park_until(|| NonNull::new(next.load(Acquire)))
                });
            }
            let mut spin = PB::default();
            if !spin.is_completed() {
                if let Some(next) = spin.try_backoff_until(|| NonNull::new(next.load(Acquire))) {
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
        abort_on_unwind(|| wait_for_next::<Self, P, PB>(next, parker))
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
impl<B: BackoffStrategy, P: Parker, PB: BoundedBackoffStrategy> Linking for AtomicEager<B, P, PB> {
    type PreferredDrainEnd = Front;
}

/// Nodes are pushed atomically to the back of the list and are linked to the previous node lazily.
///
/// A thread walking the list from the front, e.g. to unlink its front node, may need to materialize
/// the lazy linking by walking the list backward. Materialized links are cached to amortize the
/// operation. Draining the list is done backward by default, in which case materialization might
/// never happen.
///
/// Node push uses a CAS loop on the list state. Removal of the back node, or list drain, requires a
/// single RMW on the list state (in addition to the mutex locking and unlocking).
///
/// `B` is the backoff strategy used on contention when pushing nodes or updating the list state.
#[derive(Debug)]
pub struct AtomicLazy<B: BackoffStrategy = NoBackoff>(PhantomData<B>);
impl<B: BackoffStrategy> PrivateLinking for AtomicLazy<B> {
    type NextPtr = Cell<Option<NonNull<NodeLink<Self>>>>;
    type Parker = ();
    type Backoff = B;
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
    const SERIALIZED: bool = false;
    fn push_back_set_order(set_order: Ordering) -> Ordering {
        match set_order {
            Relaxed | Release => Release,
            Acquire | AcqRel => AcqRel,
            _ => SeqCst, // `Ordering` is `#[non_exhaustive]`
        }
    }
    // TODO `prev` may be freed: no projection
    fn store_next(
        _prev: *mut NodeLink<Self>,
        _head: &Self::NextPtr,
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
        #[allow(clippy::incompatible_msrv, unstable_name_collisions)]
        fn find_next<B: BackoffStrategy>(
            node: Option<NonNull<NodeLink<AtomicLazy<B>>>>,
            mut tail: NonNull<NodeLink<AtomicLazy<B>>>,
        ) -> NonNull<NodeLink<AtomicLazy<B>>> {
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
impl<B: BackoffStrategy> Linking for AtomicLazy<B> {
    type PreferredDrainEnd = Back;
}

/// Node insertion into the list is serialized by the list mutex.
///
/// Nodes can also be inserted at any position with a [`ListCursor`](crate::list::ListCursor).
#[derive(Debug)]
pub struct Serialized;
impl PrivateLinking for Serialized {
    type NextPtr = Cell<Option<NonNull<NodeLink<Self>>>>;
    type Parker = ();
    type Backoff = NoBackoff;
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
    const NODES_ACCESS_REQUIRES_TAIL_ACQUIRE: bool = false;
    const SERIALIZED: bool = true;
    fn push_back_set_order(set_order: Ordering) -> Ordering {
        set_order
    }
    #[allow(clippy::incompatible_msrv, unstable_name_collisions)]
    fn store_next(
        prev: *mut NodeLink<Self>,
        head: &Self::NextPtr,
        node: NonNull<NodeLink<Self>>,
        _parker: &Self::Parker,
    ) {
        if prev.addr() == HEAD_MARKER {
            head.set(Some(node));
        } else {
            unsafe { (*prev).next.set(Some(node)) };
        }
    }
    fn load_next(next: &Self::NextPtr) -> Option<NonNull<NodeLink<Self>>> {
        next.get()
    }
    fn get_next(
        _node: Option<NonNull<NodeLink<Self>>>,
        next: &Self::NextPtr,
        _tail: NonNull<NodeLink<Self>>,
        _parker: &Self::Parker,
    ) -> NonNull<NodeLink<Self>> {
        // TODO safety: every link is written under the mutex
        unsafe { Self::load_next(next).unwrap_unchecked() }
    }
    fn update_next(next: &Self::NextPtr, ptr: Option<NonNull<NodeLink<Self>>>) {
        next.set(ptr);
    }
    fn wait_next(next: &Self::NextPtr, _parker: &Self::Parker) -> Option<NonNull<NodeLink<Self>>> {
        Self::load_next(next)
    }
    fn drain_get_head(sentinel: &mut NodeLink<Self>) -> Option<NonNull<NodeLink<Self>>> {
        sentinel.next.get()
    }
}
impl Linking for Serialized {
    type PreferredDrainEnd = Front;
}
