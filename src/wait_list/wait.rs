use core::{
    pin::Pin,
    sync::atomic::Ordering::{Acquire, Relaxed, SeqCst},
    task::{Context, Poll},
};

use crate::{
    Node, NodeState,
    list::{Eager, Linking},
    loom::sync::atomic::fence,
    sync::mutex::{DefaultMutex, Mutex},
    wait_list::{
        ClosedError, DEFAULT_WAKER_LIST_SIZE, STATE_CLOSED, STATE_OPEN, WaitListRef,
        synchronization::{SyncMode, Synchronization, Synchronized},
    },
};

type WaitListNode<'a, S, L, M, const WAKER_LIST_SIZE: usize> =
    Node<WaitListRef<'a, S, L, M, WAKER_LIST_SIZE>>;

pub struct Wait<
    'a,
    S: Synchronization = Synchronized,
    L: Linking = Eager,
    M: Mutex = DefaultMutex,
    const WAKER_LIST_SIZE: usize = DEFAULT_WAKER_LIST_SIZE,
> {
    node: WaitListNode<'a, S, L, M, WAKER_LIST_SIZE>,
}

impl<'a, S: Synchronization, L: Linking, M: Mutex, const WAKER_LIST_SIZE: usize>
    Wait<'a, S, L, M, WAKER_LIST_SIZE>
{
    pub(super) fn new(node: WaitListNode<'a, S, L, M, WAKER_LIST_SIZE>) -> Self {
        Self { node }
    }

    fn project(self: Pin<&mut Self>) -> Pin<&mut WaitListNode<'a, S, L, M, WAKER_LIST_SIZE>> {
        unsafe { self.map_unchecked_mut(|this| &mut this.node) }
    }

    fn poll_wait(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        ignore_notification: bool,
    ) -> Poll<Result<(), ClosedError>> {
        match self.project().state() {
            NodeState::Unlinked(mut node) => {
                if node.notification.take().is_some() && !ignore_notification {
                    return Poll::Ready(Ok(()));
                }
                let set_order = match S::MODE {
                    SyncMode::Synchronized => Acquire,
                    SyncMode::Sequential => SeqCst,
                    SyncMode::Unsynchronized => Relaxed,
                };
                node.waker = Some(cx.waker().clone());
                let pushed = node.try_push_back_with(set_order, Relaxed, |_, state| {
                    if state == Some(STATE_CLOSED) {
                        // TODO synchronize with close
                        fence(Acquire);
                        return false;
                    }
                    true
                });
                if pushed {
                    Poll::Pending
                } else {
                    Poll::Ready(Err(ClosedError))
                }
            }
            NodeState::Linked(mut node) => {
                if unsafe { !node.waker.as_ref().unwrap_unchecked().will_wake(cx.waker()) } {
                    node.waker = Some(cx.waker().clone());
                }
                Poll::Pending
            }
        }
    }

    #[cold]
    #[inline(never)]
    fn unregister(self: Pin<&mut Self>) {
        match self.project().state() {
            NodeState::Unlinked(mut node) => {
                node.notification.take();
            }
            NodeState::Linked(node) => {
                debug_assert!(node.notification.is_none());
                node.unlink(|| STATE_OPEN);
            }
        }
    }
}

impl<S: Synchronization, L: Linking, M: Mutex, const WAKER_LIST_SIZE: usize> Future
    for Wait<'_, S, L, M, WAKER_LIST_SIZE>
{
    type Output = Result<(), ClosedError>;

    #[cold]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.poll_wait(cx, false)
    }
}

/// Wake condition returned by the closure passed to [`WaitList::wait_until`].
///
/// Typically implemented by `bool` and `Option<T>`. When met, it provides an output that can be
/// returned by `wait_until`.
pub trait WakeCondition {
    /// Wake condition output when met.
    type Output;
    /// Try getting the wake condition output, thereby checking if it is met.
    fn try_into_output(self) -> Option<Self::Output>;
}

impl WakeCondition for bool {
    type Output = ();
    fn try_into_output(self) -> Option<Self::Output> {
        self.then_some(())
    }
}

impl<T> WakeCondition for Option<T> {
    type Output = T;
    fn try_into_output(self) -> Option<Self::Output> {
        self
    }
}

pub struct WaitUntil<
    'a,
    F,
    S: Synchronization = Synchronized,
    L: Linking = Eager,
    M: Mutex = DefaultMutex,
    const WAKER_LIST_SIZE: usize = DEFAULT_WAKER_LIST_SIZE,
> {
    wait: Wait<'a, S, L, M, WAKER_LIST_SIZE>,
    wake_condition: F,
}

impl<
    'a,
    F: FnMut(bool) -> W,
    W: WakeCondition,
    S: Synchronization,
    L: Linking,
    M: Mutex,
    const WAKER_LIST_SIZE: usize,
> WaitUntil<'a, F, S, L, M, WAKER_LIST_SIZE>
{
    pub(super) fn new(wait: Wait<'a, S, L, M, WAKER_LIST_SIZE>, wake_condition: F) -> Self {
        Self {
            wait,
            wake_condition,
        }
    }

    fn project(self: Pin<&mut Self>) -> (Pin<&mut Wait<'a, S, L, M, WAKER_LIST_SIZE>>, &mut F) {
        let this = unsafe { self.get_unchecked_mut() };
        (
            unsafe { Pin::new_unchecked(&mut this.wait) },
            &mut this.wake_condition,
        )
    }

    #[cold]
    fn poll_wait_until(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<W::Output, ClosedError>> {
        let (mut wait, wake_condition) = self.as_mut().project();
        let is_closed = wait.as_mut().poll_wait(cx, true).is_ready();
        match (wake_condition)(true).try_into_output() {
            Some(res) => {
                wait.unregister();
                Poll::Ready(Ok(res))
            }
            None if is_closed => Poll::Ready(Err(ClosedError)),
            None => Poll::Pending,
        }
    }
}

impl<
    F: FnMut(bool) -> W,
    W: WakeCondition,
    S: Synchronization,
    L: Linking,
    M: Mutex,
    const WAKER_LIST_SIZE: usize,
> Future for WaitUntil<'_, F, S, L, M, WAKER_LIST_SIZE>
{
    type Output = Result<W::Output, ClosedError>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (wait, wake_condition) = self.as_mut().project();
        match (wake_condition)(false).try_into_output() {
            Some(res) => {
                if wait.node.is_maybe_linked() {
                    wait.unregister();
                }
                Poll::Ready(Ok(res))
            }
            None => self.poll_wait_until(cx),
        }
    }
}
