use core::{fmt::Debug, ptr::NonNull};

#[allow(unused_imports)]
use crate::msrv::StrictProvenance;
use crate::{list::Linking, msrv::ptr, node::NodeLink};

mod private {
    use core::{marker::PhantomData, ptr::NonNull};

    use crate::{list::Linking, node::NodeLink};

    pub struct Tail<S, L>(PhantomData<(S, L)>);

    /// # Safety
    ///
    /// Implementation must be bijective.
    pub unsafe trait ListStatePrivate: Sized {
        fn tail_to_enum<L: Linking>(tail: *mut Tail<Self, L>) -> StateOrPtr<Self, L>;
        fn enum_to_tail<L: Linking>(state_or_ptr: StateOrPtr<Self, L>) -> *mut Tail<Self, L>;
    }

    pub enum StateOrPtr<S, L: Linking> {
        State(S),
        Ptr(NonNull<NodeLink<L>>),
    }
}
pub(super) use private::{ListStatePrivate, StateOrPtr, Tail};

pub trait ListState: ListStatePrivate + Debug + Copy + PartialEq + Send + Sync + 'static {}

impl<S: Copy, L: Linking> Clone for StateOrPtr<S, L> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<S: Copy, L: Linking> Copy for StateOrPtr<S, L> {}

impl<S, L: Linking> StateOrPtr<S, L> {
    pub(super) fn state(self) -> Option<S> {
        match self {
            Self::State(state) => Some(state),
            _ => None,
        }
    }
}

unsafe impl ListStatePrivate for () {
    #[inline(always)]
    fn tail_to_enum<L: Linking>(tail: *mut Tail<Self, L>) -> StateOrPtr<Self, L> {
        NonNull::new(tail.cast()).map_or(StateOrPtr::State(()), StateOrPtr::Ptr)
    }
    #[inline(always)]
    fn enum_to_tail<L: Linking>(state_or_ptr: StateOrPtr<Self, L>) -> *mut Tail<Self, L> {
        match state_or_ptr {
            StateOrPtr::State(_) => ptr::null_mut(),
            StateOrPtr::Ptr(ptr) => ptr.as_ptr().cast(),
        }
    }
}
impl ListState for () {}

const TAIL_FLAG: usize = 1;
const STATE_SHIFT: usize = 1;

pub const LIST_STATE_MAX: usize = usize::MAX >> STATE_SHIFT;
#[inline(always)]
pub(super) const fn state_to_ptr<L: Linking>(state: usize) -> *mut Tail<usize, L> {
    #[cold]
    #[inline(never)]
    const fn panic_list_state_overflow() -> ! {
        panic!("list state overflow")
    }
    if state > LIST_STATE_MAX {
        panic_list_state_overflow()
    }
    ptr::without_provenance_mut(state << STATE_SHIFT)
}

unsafe impl ListStatePrivate for usize {
    #[inline(always)]
    #[allow(clippy::incompatible_msrv, unstable_name_collisions)]
    fn tail_to_enum<L: Linking>(tail: *mut Tail<Self, L>) -> StateOrPtr<Self, L> {
        if tail.addr() & TAIL_FLAG != 0 {
            let ptr = tail.map_addr(|addr| addr & !TAIL_FLAG).cast();
            StateOrPtr::Ptr(unsafe { NonNull::new_unchecked(ptr) })
        } else {
            StateOrPtr::State(tail.addr() >> STATE_SHIFT)
        }
    }
    #[inline(always)]
    #[allow(clippy::incompatible_msrv, unstable_name_collisions)]
    fn enum_to_tail<L: Linking>(state_or_ptr: StateOrPtr<Self, L>) -> *mut Tail<Self, L> {
        match state_or_ptr {
            StateOrPtr::State(state) => state_to_ptr(state),
            StateOrPtr::Ptr(ptr) => ptr.as_ptr().map_addr(|addr| addr | TAIL_FLAG).cast(),
        }
    }
}
impl ListState for usize {}

pub(super) trait TailExt<S, L: Linking> {
    fn state(self) -> Option<S>;
    fn ptr(self) -> Option<NonNull<NodeLink<L>>>;
}

impl<S: ListState, L: Linking> TailExt<S, L> for *mut Tail<S, L> {
    #[inline(always)]
    fn state(self) -> Option<S> {
        match S::tail_to_enum::<L>(self) {
            StateOrPtr::State(state) => Some(state),
            _ => None,
        }
    }

    #[inline(always)]
    fn ptr(self) -> Option<NonNull<NodeLink<L>>> {
        match S::tail_to_enum::<L>(self) {
            StateOrPtr::Ptr(ptr) => Some(ptr),
            _ => None,
        }
    }
}

pub(super) trait IntoTail<S, L: Linking> {
    fn into_tail(self) -> *mut Tail<S, L>;
}

impl<S: ListState, L: Linking> IntoTail<S, L> for S {
    #[inline(always)]
    fn into_tail(self) -> *mut Tail<S, L> {
        S::enum_to_tail(StateOrPtr::State(self))
    }
}

impl<S: ListState, L: Linking> IntoTail<S, L> for NonNull<NodeLink<L>> {
    #[inline(always)]
    fn into_tail(self) -> *mut Tail<S, L> {
        S::enum_to_tail(StateOrPtr::Ptr(self))
    }
}
