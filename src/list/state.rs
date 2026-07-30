use core::{hint, marker::PhantomData, ptr, ptr::NonNull};

use crate::node::NodeLink;

pub(super) struct Tail<S>(PhantomData<S>);

#[expect(private_bounds)]
pub trait ListState: QueueStatePrivate + Copy + PartialEq + 'static {}

/// # Safety
///
/// Implementation must be bijective.
pub(super) unsafe trait QueueStatePrivate: Sized {
    fn tail_to_enum(tail: *mut Tail<Self>) -> StateOrPtr<Self>;
    fn enum_to_tail(state_or_ptr: StateOrPtr<Self>) -> *mut Tail<Self>;
    fn tail_to_state_or(tail: *mut Tail<Self>, default: Self) -> Self;
}

#[derive(Clone, Copy)]
pub(super) enum StateOrPtr<S> {
    State(S),
    Ptr(NonNull<NodeLink>),
}

impl<S> StateOrPtr<S> {
    pub(super) fn state(self) -> Option<S> {
        match self {
            Self::State(state) => Some(state),
            _ => None,
        }
    }
}

unsafe impl QueueStatePrivate for () {
    #[inline(always)]
    fn tail_to_enum(tail: *mut Tail<Self>) -> StateOrPtr<Self> {
        NonNull::new(tail.cast()).map_or(StateOrPtr::State(()), StateOrPtr::Ptr)
    }
    #[inline(always)]
    fn enum_to_tail(state_or_ptr: StateOrPtr<Self>) -> *mut Tail<Self> {
        match state_or_ptr {
            StateOrPtr::State(_) => ptr::null_mut(),
            StateOrPtr::Ptr(ptr) => ptr.as_ptr().cast(),
        }
    }
    #[inline(always)]
    fn tail_to_state_or(_tail: *mut Tail<Self>, _default: Self) -> Self {}
}
impl ListState for () {}

const TAIL_FLAG: usize = 1;
const STATE_SHIFT: usize = 1;

pub const INTRUSIVE_QUEUE_MAX_STATE: usize = usize::MAX >> STATE_SHIFT;
#[inline(always)]
pub(super) const fn state_to_ptr(state: usize) -> *mut Tail<usize> {
    #[cold]
    #[inline(never)]
    const fn panic_queue_state_overflow() -> ! {
        panic!("list state overflow")
    }
    if state > INTRUSIVE_QUEUE_MAX_STATE {
        panic_queue_state_overflow()
    }
    ptr::without_provenance_mut(state << STATE_SHIFT)
}

unsafe impl QueueStatePrivate for usize {
    #[inline(always)]
    fn tail_to_enum(tail: *mut Tail<Self>) -> StateOrPtr<Self> {
        if tail.addr() & TAIL_FLAG != 0 {
            let ptr = tail.map_addr(|addr| addr & !TAIL_FLAG).cast();
            StateOrPtr::Ptr(unsafe { NonNull::new_unchecked(ptr) })
        } else {
            StateOrPtr::State(tail.addr() >> STATE_SHIFT)
        }
    }
    #[inline(always)]
    fn enum_to_tail(state_or_ptr: StateOrPtr<Self>) -> *mut Tail<Self> {
        match state_or_ptr {
            StateOrPtr::State(state) => state_to_ptr(state),
            StateOrPtr::Ptr(ptr) => ptr.as_ptr().map_addr(|addr| addr | TAIL_FLAG).cast(),
        }
    }

    #[inline(always)]
    fn tail_to_state_or(tail: *mut Tail<Self>, default: Self) -> Self {
        hint::select_unpredictable(
            tail.addr() & TAIL_FLAG == 0,
            tail.addr() >> STATE_SHIFT,
            default,
        )
    }
}
impl ListState for usize {}

pub(super) trait TailExt<S> {
    fn state(self) -> Option<S>;
    fn ptr(self) -> Option<NonNull<NodeLink>>;
}

impl<S: ListState> TailExt<S> for *mut Tail<S> {
    #[inline(always)]
    fn state(self) -> Option<S> {
        match S::tail_to_enum(self) {
            StateOrPtr::State(state) => Some(state),
            _ => None,
        }
    }

    #[inline(always)]
    fn ptr(self) -> Option<NonNull<NodeLink>> {
        match S::tail_to_enum(self) {
            StateOrPtr::Ptr(ptr) => Some(ptr),
            _ => None,
        }
    }
}

pub(super) trait IntoTail<S> {
    fn into_tail(self) -> *mut Tail<S>;
}

impl<S: ListState> IntoTail<S> for S {
    #[inline(always)]
    fn into_tail(self) -> *mut Tail<S> {
        S::enum_to_tail(StateOrPtr::State(self))
    }
}

impl<S: ListState> IntoTail<S> for NonNull<NodeLink> {
    #[inline(always)]
    fn into_tail(self) -> *mut Tail<S> {
        S::enum_to_tail(StateOrPtr::Ptr(self))
    }
}
