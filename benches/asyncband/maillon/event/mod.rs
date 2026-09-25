use std::sync::atomic::{
    AtomicBool,
    Ordering::{Acquire, Release},
};

use maillon::{
    WaitList,
    linking::{AtomicEager, AtomicLazy, Linking},
    wait_list::synchronization::Synchronized,
};

mod eager {
    type ManualResetEvent = super::ManualResetEvent<super::AtomicEager>;
    #[allow(clippy::duplicate_mod)]
    #[path = "../wait.rs"]
    mod wait;
}

mod lazy {
    type ManualResetEvent = super::ManualResetEvent<super::AtomicLazy>;
    #[allow(clippy::duplicate_mod)]
    #[path = "../wait.rs"]
    mod wait;
}

pub struct ManualResetEvent<L: Linking> {
    is_set: AtomicBool,
    waiters: WaitList<(), Synchronized, L>,
}

impl<L: Linking> ManualResetEvent<L> {
    pub fn new() -> Self {
        Self::with_state(false)
    }

    pub fn with_state(is_set: bool) -> Self {
        Self {
            is_set: AtomicBool::new(is_set),
            waiters: WaitList::new(),
        }
    }

    pub fn set(&self) {
        self.is_set.store(true, Release);
        self.waiters.notify_all();
    }

    pub fn reset(&self) {
        self.is_set.store(false, Release);
    }

    pub fn is_set(&self) -> bool {
        self.is_set.load(Acquire)
    }

    pub async fn wait(&self) {
        let is_set = |_| self.is_set.load(Acquire);
        let _ = self.waiters.wait_until_with((), is_set, |()| true).await;
    }
}
