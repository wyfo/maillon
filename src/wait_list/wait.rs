//! The futures returned by [`WaitList::wait`](crate::WaitList::wait) and
//! [`WaitList::wait_until`](crate::WaitList::wait_until).
use core::{
    future::Future,
    pin::Pin,
    sync::atomic::Ordering::{Acquire, Relaxed, SeqCst},
    task::{Context, Poll},
};

#[allow(unused_imports)]
use crate::msrv::OptionExt;
use crate::{
    Node, NodeState,
    linking::{AtomicEager, Linking},
    loom::sync::atomic::fence,
    node_wrapper,
    sync::mutex::{DefaultMutex, Mutex},
    wait_list::{
        ClosedError, DEFAULT_WAKER_BATCH_SIZE, Notification, STATE_CLOSED, STATE_OPEN, WaitListRef,
        synchronization::{SyncMode, Synchronization, Synchronized},
    },
};

node_wrapper! {
    /// Future returned by [`WaitList::wait_with`].
    ///
    /// Once completed, the future can be polled again to restart waiting.
    ///
    /// [`WaitList::wait_with`]: crate::WaitList::wait_with
    pub struct Wait<
        'a,
        N: Notification = (),
        S: Synchronization = Synchronized,
        L: Linking = AtomicEager,
        M: Mutex = DefaultMutex,
        const WAKER_BATCH_SIZE: usize = DEFAULT_WAKER_BATCH_SIZE,
    >(pub(super) Node<WaitListRef<'a, N, S, L, M, WAKER_BATCH_SIZE>>);
}

impl<N: Notification, S: Synchronization, L: Linking, M: Mutex, const WAKER_BATCH_SIZE: usize>
    Wait<'_, N, S, L, M, WAKER_BATCH_SIZE>
{
    #[cold]
    #[inline(never)]
    fn unregister(self: Pin<&mut Self>) {
        match self.node_mut().state() {
            NodeState::Unlinked(mut node) => {
                node.notification.take();
            }
            NodeState::Linked(node) => {
                debug_assert!(node.notification.is_none());
                node.unlink(|_, _| STATE_OPEN);
            }
        }
    }
}

impl<N: Notification, S: Synchronization, L: Linking, M: Mutex, const WAKER_BATCH_SIZE: usize>
    Future for Wait<'_, N, S, L, M, WAKER_BATCH_SIZE>
{
    type Output = Result<N, ClosedError>;

    #[cold]
    #[allow(clippy::incompatible_msrv)]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.node_mut().state() {
            NodeState::Unlinked(mut node) => {
                if let Some(notification) = node.notification.take() {
                    return Poll::Ready(Ok(notification.into_inner()));
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
                if node.waker.as_ref().is_none_or(|w| !w.will_wake(cx.waker())) {
                    node.waker = Some(cx.waker().clone());
                }
                Poll::Pending
            }
        }
    }
}

/// Wake condition returned by the closures passed to [`WaitList::wait_until_with`].
///
/// Implemented for `bool` (with `()` output) and `Option<T>` (with `T` output). When satisfied, it
/// provides an output that is returned by `wait_until_with`.
///
/// The [`Default`] implementation should return an unsatisfied condition.
///
/// [`WaitList::wait_until_with`]: crate::WaitList::wait_until_with
pub trait WakeCondition: Default {
    /// Wake condition output when satisfied.
    type Output;
    /// Returns the output if the wake condition is satisfied.
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

/// Future returned by [`WaitList::wait_until`](crate::WaitList::wait_until).
pub struct WaitUntil<
    'a,
    F,
    G,
    N: Notification = (),
    S: Synchronization = Synchronized,
    L: Linking = AtomicEager,
    M: Mutex = DefaultMutex,
    const WAKER_BATCH_SIZE: usize = DEFAULT_WAKER_BATCH_SIZE,
> {
    wait: Wait<'a, N, S, L, M, WAKER_BATCH_SIZE>,
    wake_condition: F,
    on_notification: G,
}

impl<
    'a,
    W: WakeCondition,
    F: FnMut(bool) -> W,
    G: FnMut(N) -> W,
    N: Notification,
    S: Synchronization,
    L: Linking,
    M: Mutex,
    const WAKER_BATCH_SIZE: usize,
> WaitUntil<'a, F, G, N, S, L, M, WAKER_BATCH_SIZE>
{
    pub(super) fn new(
        wait: Wait<'a, N, S, L, M, WAKER_BATCH_SIZE>,
        wake_condition: F,
        on_notification: G,
    ) -> Self {
        Self {
            wait,
            wake_condition,
            on_notification,
        }
    }

    #[allow(clippy::type_complexity)]
    fn project(
        self: Pin<&mut Self>,
    ) -> (
        Pin<&mut Wait<'a, N, S, L, M, WAKER_BATCH_SIZE>>,
        &mut F,
        &mut G,
    ) {
        let this = unsafe { self.get_unchecked_mut() };
        (
            unsafe { Pin::new_unchecked(&mut this.wait) },
            &mut this.wake_condition,
            &mut this.on_notification,
        )
    }

    #[cold]
    fn poll_cold(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<W::Output, ClosedError>> {
        let (mut wait, wake_condition, on_notification) = self.as_mut().project();
        let mut wait_res = wait.as_mut().poll(cx);
        if let Poll::Ready(Ok(notification)) = wait_res {
            if let Some(res) = on_notification(notification).try_into_output() {
                return Poll::Ready(Ok(res));
            }
            wait_res = wait.as_mut().poll(cx);
        }
        debug_assert!(matches!(
            wait_res,
            Poll::Pending | Poll::Ready(Err(ClosedError))
        ));
        match (wake_condition)(true).try_into_output() {
            Some(res) => {
                wait.unregister();
                Poll::Ready(Ok(res))
            }
            None if wait_res.is_ready() => Poll::Ready(Err(ClosedError)),
            None => Poll::Pending,
        }
    }
}

impl<
    W: WakeCondition,
    F: FnMut(bool) -> W,
    G: FnMut(N) -> W,
    N: Notification,
    S: Synchronization,
    L: Linking,
    M: Mutex,
    const WAKER_BATCH_SIZE: usize,
> Future for WaitUntil<'_, F, G, N, S, L, M, WAKER_BATCH_SIZE>
{
    type Output = Result<W::Output, ClosedError>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (wait, wake_condition, _) = self.as_mut().project();
        match (wake_condition)(false).try_into_output() {
            Some(res) => {
                if wait.node().is_maybe_linked() {
                    wait.unregister();
                }
                Poll::Ready(Ok(res))
            }
            None => self.poll_cold(cx),
        }
    }
}
