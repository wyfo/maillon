use std::{
    future::Future,
    pin::Pin,
    sync::atomic::Ordering::{Acquire, Relaxed, Release},
    task::{Context, Poll, Waker},
};

use maillon::{
    List, LockedList, Node, NodeData, NodeState,
    linking::{AtomicEager, AtomicLazy, Linking},
    node_wrapper,
};

mod eager {
    type ManualResetEvent = super::ManualResetEvent<super::AtomicEager>;
    #[allow(clippy::duplicate_mod)]
    #[path = "../../event/wait.rs"]
    mod wait;
}

mod lazy {
    type ManualResetEvent = super::ManualResetEvent<super::AtomicLazy>;
    #[allow(clippy::duplicate_mod)]
    #[path = "../../event/wait.rs"]
    mod wait;
}

const UNSET: usize = 0;
const SET: usize = 1;
const WAKER_BATCH_SIZE: usize = 32;

type WaiterList<L> = List<Waiter, usize, (), L>;

pub struct ManualResetEvent<L: Linking>(WaiterList<L>);

impl<L: Linking> ManualResetEvent<L> {
    pub fn new() -> Self {
        Self::with_state(false)
    }

    pub fn with_state(is_set: bool) -> Self {
        Self(List::with_state(if is_set { SET } else { UNSET }))
    }

    pub fn set(&self) {
        (self.0).update_state_or_lock_with(
            Release,
            Relaxed,
            |_| SET,
            |locked| {
                locked
                    .drain(|_| SET)
                    .wake_all::<WAKER_BATCH_SIZE, _>(|mut waiter, _| {
                        waiter.notified = true;
                        waiter.waker.take()
                    });
            },
        );
    }

    pub fn reset(&self) {
        let _ = (self.0).compare_exchange_state(SET, UNSET, Relaxed, Relaxed);
    }

    pub fn is_set(&self) -> bool {
        self.0.load_state(Acquire) == Some(SET)
    }

    pub fn wait(&self) -> Wait<'_, L> {
        Wait(Node::new(&self.0))
    }
}

#[derive(Default)]
struct Waiter {
    notified: bool,
    waker: Option<Waker>,
}

impl<L: Linking> NodeData<&WaiterList<L>> for Waiter {
    fn new_state_if_last_node_on_drop(
        self: Pin<&mut Self>,
        _list: &&WaiterList<L>,
        _list_data: &mut (),
    ) -> usize {
        UNSET
    }

    fn on_drop<'list>(
        self: Pin<&mut Self>,
        _list: &'list &WaiterList<L>,
        _locked: Option<LockedList<'list, Self, usize, (), L>>,
        _state_updated_on_unlink: bool,
    ) {
    }
}

node_wrapper! {
    pub struct Wait<'a, L: Linking>(Node<&'a WaiterList<L>>);
}

impl<L: Linking> Future for Wait<'_, L> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        match self.node_mut().state() {
            NodeState::Unlinked(node) => {
                if node.notified {
                    return Poll::Ready(());
                }
                let pushed = node.try_push_back_with(Relaxed, Acquire, |mut waiter, state| {
                    if state == Some(SET) {
                        return false;
                    }
                    waiter.waker.get_or_insert_with(|| cx.waker().clone());
                    true
                });
                if pushed {
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            }
            NodeState::Linked(mut node) => node.update_waker(cx, |n| &mut n.waker),
        }
    }
}
