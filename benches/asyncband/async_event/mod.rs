use std::sync::atomic::{
    AtomicBool,
    Ordering::{Acquire, Release},
};

mod event;

pub struct ManualResetEvent {
    is_set: AtomicBool,
    event: ::async_event::Event,
}

impl ManualResetEvent {
    pub fn new() -> Self {
        Self::with_state(false)
    }

    pub fn with_state(is_set: bool) -> Self {
        Self {
            is_set: AtomicBool::new(is_set),
            event: ::async_event::Event::new(),
        }
    }

    pub fn set(&self) {
        self.is_set.store(true, Release);
        self.event.notify_all();
    }

    pub fn reset(&self) {
        self.is_set.store(false, Release);
    }

    pub fn is_set(&self) -> bool {
        self.is_set.load(Acquire)
    }

    pub async fn wait(&self) {
        (self.event)
            .wait_until(|| self.is_set.load(Acquire).then_some(()))
            .await;
    }
}
