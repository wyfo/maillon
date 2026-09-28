//! [`Linking`] and its variants.
use core::{
    marker::PhantomData,
    num::NonZeroUsize,
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
        type Backoff: BackoffStrategy;
        #[cfg(not(loom))]
        const NEW_NEXT: Self::NextPtr;
        #[cfg(not(loom))]
        const INIT: Self;
        fn new_next(ptr: Option<NonNull<NodeLink<Self>>>) -> Self::NextPtr;
        #[cfg(loom)]
        fn new() -> Self;
        const LAZY: bool;
        const SERIALIZED: bool;
        fn push_back_set_order(set_order: Ordering) -> Ordering;
        fn store_next(
            &self,
            prev: *mut NodeLink<Self>,
            head: &Self::NextPtr,
            node: NonNull<NodeLink<Self>>,
        );
        fn load_next(next: &Self::NextPtr) -> Option<NonNull<NodeLink<Self>>>;
        fn get_next(
            &self,
            node: Option<NonNull<NodeLink<Self>>>,
            next: &Self::NextPtr,
            tail: NonNull<NodeLink<Self>>,
        ) -> NonNull<NodeLink<Self>>;
        fn unlink(
            &self,
            _node: NonNull<NodeLink<Self>>,
            _prev: NonNull<NodeLink<Self>>,
            _next: Option<NonNull<NodeLink<Self>>>,
        ) {
        }
        fn update_next(next: &Self::NextPtr, ptr: Option<NonNull<NodeLink<Self>>>);
        fn wait_next(&self, next: &Self::NextPtr) -> Option<NonNull<NodeLink<Self>>>;
        fn drain_get_head(&self, sentinel: &mut NodeLink<Self>) -> Option<NonNull<NodeLink<Self>>>;
        fn drain_set_head(sentinel: &mut NodeLink<Self>, head: Option<NonNull<NodeLink<Self>>>) {
            Self::update_next(&sentinel.next, head);
        }
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
const NOT_MATERIALIZED_TAG: usize = 1;

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
> {
    parker: P,
    _phantom: PhantomData<(B, PB)>,
}
impl<B: BackoffStrategy, P: Parker, PB: BoundedBackoffStrategy> PrivateLinking
    for AtomicEager<B, P, PB>
{
    type NextPtr = AtomicPtr<NodeLink<Self>>;
    type Backoff = B;
    #[allow(clippy::declare_interior_mutable_const)]
    #[cfg(not(loom))]
    const NEW_NEXT: Self::NextPtr = AtomicPtr::new(ptr::null_mut());
    #[allow(clippy::declare_interior_mutable_const)]
    #[cfg(not(loom))]
    const INIT: Self = Self {
        parker: P::INIT,
        _phantom: PhantomData,
    };
    fn new_next(ptr: Option<NonNull<NodeLink<Self>>>) -> Self::NextPtr {
        AtomicPtr::new(ptr.as_ptr())
    }
    #[cfg(loom)]
    fn new() -> Self {
        Self {
            parker: P::new(),
            _phantom: PhantomData,
        }
    }
    const LAZY: bool = false;
    const SERIALIZED: bool = false;
    fn push_back_set_order(set_order: Ordering) -> Ordering {
        match set_order {
            Relaxed | Acquire | Release | AcqRel => AcqRel,
            _ => SeqCst, // `Ordering` is `#[non_exhaustive]`
        }
    }
    #[allow(clippy::incompatible_msrv, unstable_name_collisions)]
    fn store_next(
        &self,
        prev: *mut NodeLink<Self>,
        head: &Self::NextPtr,
        node: NonNull<NodeLink<Self>>,
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
                    // Parker must not unwind, as node.linked_list would not bet set otherwise
                    // so the node would not be removed in drop.
                    abort_on_unwind(|| unsafe {
                        parker.unpark(tagged_parked_state.map_addr(|addr| addr & !PARKED_TAG));
                    });
                }
                unpark(&self.parker, tagged_parked_state.cast());
            }
        }
    }
    fn load_next(next: &Self::NextPtr) -> Option<NonNull<NodeLink<Self>>> {
        NonNull::new(next.load(Acquire))
    }
    fn get_next(
        &self,
        _node: Option<NonNull<NodeLink<Self>>>,
        next: &Self::NextPtr,
        _tail: NonNull<NodeLink<Self>>,
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
        abort_on_unwind(|| wait_for_next::<Self, P, PB>(next, &self.parker))
    }
    fn update_next(next: &Self::NextPtr, ptr: Option<NonNull<NodeLink<Self>>>) {
        next.store(ptr.as_ptr(), Relaxed);
    }
    fn wait_next(&self, next: &Self::NextPtr) -> Option<NonNull<NodeLink<Self>>> {
        Some(self.get_next(None, next, NonNull::dangling()))
    }
    fn drain_get_head(&self, sentinel: &mut NodeLink<Self>) -> Option<NonNull<NodeLink<Self>>> {
        NonNull::new(sentinel.next.load_mut())
    }
    fn drain_set_head(sentinel: &mut NodeLink<Self>, head: Option<NonNull<NodeLink<Self>>>) {
        sentinel.next.store_mut(head.as_ptr());
    }
}
impl<B: BackoffStrategy, P: Parker, PB: BoundedBackoffStrategy> Linking for AtomicEager<B, P, PB> {
    type PreferredDrainEnd = Front;
}

/// Nodes are pushed atomically to the back of the list and are linked to the previous node lazily.
///
/// A thread walking the list from the front, e.g. to unlink its front node, may need to materialize
/// the lazy linking by walking the list backward. Materialized links are cached to amortize the
/// operation. Draining the list is done backward by default, in which case materialization only
/// happens if the list is unlocked mid-drain.
///
/// Node push uses a CAS loop on the list state. Removal of the back node, or list drain, requires a
/// single RMW on the list state (in addition to the mutex locking and unlocking).
///
/// `B` is the backoff strategy used on contention when pushing nodes or updating the list state.
#[derive(Debug)]
pub struct AtomicLazy<B: BackoffStrategy = NoBackoff> {
    cache: Cell<Option<NonNull<NodeLink<Self>>>>,
    _phantom: PhantomData<B>,
}
unsafe impl<B: BackoffStrategy + Send> Send for AtomicLazy<B> {}
unsafe impl<B: BackoffStrategy + Sync> Sync for AtomicLazy<B> {}
impl<B: BackoffStrategy> AtomicLazy<B> {
    #[cold]
    #[inline(never)]
    #[allow(clippy::incompatible_msrv, unstable_name_collisions)]
    fn find_next(
        &self,
        node: Option<NonNull<NodeLink<Self>>>,
        mut from: NonNull<NodeLink<Self>>,
    ) -> Option<NonNull<NodeLink<Self>>> {
        let mut found = None;
        loop {
            let prev = unsafe { from.as_ref().load_prev() };
            if prev.addr().get() == HEAD_MARKER {
                debug_assert!(node.is_none());
                self.cache.set(None);
                return Some(from);
            }
            let prev_ref = unsafe { prev.as_ref() };
            if prev_ref.next.get().is_some() {
                if found.is_some() {
                    return found;
                }
                from = self.cache.take()?;
                continue;
            }
            prev_ref.next.set(Some(from));
            if Some(prev) == node {
                if self.cache.get().is_none() {
                    self.cache.set(Some(prev));
                    return Some(from);
                }
                found = Some(from);
            }
            // It's possible to have a cached node without its next pointer set
            // if its next node was previously removed and another node was pushed later
            // (this cached node might also be the one searched for)
            if Some(prev) == self.cache.get() {
                if found.is_some() {
                    return found;
                }
                self.cache.set(None);
            }
            from = prev;
        }
    }
}
impl<B: BackoffStrategy> PrivateLinking for AtomicLazy<B> {
    type NextPtr = Cell<Option<NonNull<NodeLink<Self>>>>;
    type Backoff = B;
    #[allow(clippy::declare_interior_mutable_const)]
    #[cfg(not(loom))]
    const NEW_NEXT: Self::NextPtr = Cell::new(None);
    #[allow(clippy::declare_interior_mutable_const)]
    #[cfg(not(loom))]
    const INIT: Self = Self {
        cache: Cell::new(None),
        _phantom: PhantomData,
    };
    fn new_next(ptr: Option<NonNull<NodeLink<Self>>>) -> Self::NextPtr {
        Cell::new(ptr)
    }
    #[cfg(loom)]
    fn new() -> Self {
        Self {
            cache: Cell::new(None),
            _phantom: PhantomData,
        }
    }
    const LAZY: bool = true;
    const SERIALIZED: bool = false;
    fn push_back_set_order(set_order: Ordering) -> Ordering {
        match set_order {
            Relaxed | Release => Release,
            Acquire | AcqRel => AcqRel,
            _ => SeqCst, // `Ordering` is `#[non_exhaustive]`
        }
    }
    fn store_next(
        &self,
        _prev: *mut NodeLink<Self>,
        _head: &Self::NextPtr,
        _node: NonNull<NodeLink<Self>>,
    ) {
    }
    fn load_next(next: &Self::NextPtr) -> Option<NonNull<NodeLink<Self>>> {
        next.get()
    }
    fn get_next(
        &self,
        node: Option<NonNull<NodeLink<Self>>>,
        next: &Self::NextPtr,
        tail: NonNull<NodeLink<Self>>,
    ) -> NonNull<NodeLink<Self>> {
        debug_assert_ne!(node, Some(tail));
        if let Some(next) = Self::load_next(next) {
            return next;
        }
        let from = match node {
            Some(_) => tail,
            None => self.cache.get().unwrap_or(tail),
        };
        let found = unsafe { self.find_next(node, from).unwrap_unchecked() };
        if node.is_none() {
            debug_assert!(self.cache.get().is_none());
            next.set(Some(found));
        }
        found
    }
    #[allow(clippy::incompatible_msrv, unstable_name_collisions)]
    fn unlink(
        &self,
        node: NonNull<NodeLink<Self>>,
        prev: NonNull<NodeLink<Self>>,
        next: Option<NonNull<NodeLink<Self>>>,
    ) {
        let cached = self.cache.get();
        if cached == Some(node) {
            let is_head = prev.addr().get() == HEAD_MARKER;
            self.cache.set((next.is_some() && !is_head).then_some(prev));
        }
    }
    fn update_next(next: &Self::NextPtr, ptr: Option<NonNull<NodeLink<Self>>>) {
        next.set(ptr);
    }
    fn wait_next(&self, next: &Self::NextPtr) -> Option<NonNull<NodeLink<Self>>> {
        Self::load_next(next)
    }
    #[allow(clippy::incompatible_msrv, unstable_name_collisions)]
    fn drain_get_head(&self, sentinel: &mut NodeLink<Self>) -> Option<NonNull<NodeLink<Self>>> {
        let head = sentinel.next.get();
        if let Some(head) = head {
            if head.addr().get() & NOT_MATERIALIZED_TAG == 0 {
                return Some(head);
            }
        }
        let head = self
            .find_next(None, NonNull::new(sentinel.prev.load_mut())?)
            .unwrap_or_else(|| unsafe {
                head.unwrap_unchecked().map_addr(|addr| {
                    NonZeroUsize::new_unchecked(addr.get() & !NOT_MATERIALIZED_TAG)
                })
            });
        sentinel.next.set(Some(head));
        Some(head)
    }
    #[allow(clippy::incompatible_msrv, unstable_name_collisions)]
    fn drain_set_head(sentinel: &mut NodeLink<Self>, head: Option<NonNull<NodeLink<Self>>>) {
        if sentinel.next.get().is_none() {
            (sentinel.next).set(head.map(|h| h.map_addr(|addr| addr | NOT_MATERIALIZED_TAG)));
        } else {
            debug_assert!(
                head.is_none()
                    || matches!(sentinel.next.get(), Some(n) if n.addr().get() & NOT_MATERIALIZED_TAG == 0)
            );
            sentinel.next.set(head);
        }
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
    type Backoff = NoBackoff;
    #[allow(clippy::declare_interior_mutable_const)]
    #[cfg(not(loom))]
    const NEW_NEXT: Self::NextPtr = Cell::new(None);
    #[cfg(not(loom))]
    const INIT: Self = Self;
    fn new_next(ptr: Option<NonNull<NodeLink<Self>>>) -> Self::NextPtr {
        Cell::new(ptr)
    }
    #[cfg(loom)]
    fn new() -> Self {
        Self
    }
    const LAZY: bool = false;
    const SERIALIZED: bool = true;
    fn push_back_set_order(set_order: Ordering) -> Ordering {
        set_order
    }
    #[allow(clippy::incompatible_msrv, unstable_name_collisions)]
    fn store_next(
        &self,
        prev: *mut NodeLink<Self>,
        head: &Self::NextPtr,
        node: NonNull<NodeLink<Self>>,
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
        &self,
        _node: Option<NonNull<NodeLink<Self>>>,
        next: &Self::NextPtr,
        _tail: NonNull<NodeLink<Self>>,
    ) -> NonNull<NodeLink<Self>> {
        unsafe { Self::load_next(next).unwrap_unchecked() }
    }
    fn update_next(next: &Self::NextPtr, ptr: Option<NonNull<NodeLink<Self>>>) {
        next.set(ptr);
    }
    fn wait_next(&self, next: &Self::NextPtr) -> Option<NonNull<NodeLink<Self>>> {
        Self::load_next(next)
    }
    fn drain_get_head(&self, sentinel: &mut NodeLink<Self>) -> Option<NonNull<NodeLink<Self>>> {
        sentinel.next.get()
    }
}
impl Linking for Serialized {
    type PreferredDrainEnd = Front;
}
