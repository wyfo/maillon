use std::sync::{Arc, atomic::AtomicUsize};

use concurrent_queue::{ConcurrentQueue, PopError, PushError};
use event_listener::Event;
use maillon::{WaitList, wait_list::synchronization::Synchronization};

pub trait Notify: Default {
    fn notify_additional_one(&self);
    fn notify_all(&self);
}

impl Notify for async_event::Event {
    #[inline]
    fn notify_additional_one(&self) {
        self.notify_one();
    }

    #[inline]
    fn notify_all(&self) {
        async_event::Event::notify_all(self);
    }
}

impl Notify for Event {
    #[inline]
    fn notify_additional_one(&self) {
        self.notify_additional(1);
    }

    #[inline]
    fn notify_all(&self) {
        self.notify(usize::MAX);
    }
}

impl<S: Synchronization> Notify for WaitList<(), S> {
    #[inline]
    fn notify_additional_one(&self) {
        self.notify_one();
    }

    #[inline]
    fn notify_all(&self) {
        WaitList::notify_all(self);
    }
}

#[allow(dead_code)]
struct Channel<T, E> {
    queue: ConcurrentQueue<T>,
    send_ops: E,
    recv_ops: E,
    stream_ops: E,
    closed_ops: E,
    sender_count: AtomicUsize,
    receiver_count: AtomicUsize,
}

pub struct Sender<T, E> {
    channel: Arc<Channel<T, E>>,
}

pub struct Receiver<T, E> {
    channel: Arc<Channel<T, E>>,
}

fn with_queue<T, E: Notify>(queue: ConcurrentQueue<T>) -> (Sender<T, E>, Receiver<T, E>) {
    let channel = Arc::new(Channel {
        queue,
        send_ops: E::default(),
        recv_ops: E::default(),
        stream_ops: E::default(),
        closed_ops: E::default(),
        sender_count: AtomicUsize::new(1),
        receiver_count: AtomicUsize::new(1),
    });
    let s = Sender {
        channel: channel.clone(),
    };
    let r = Receiver { channel };
    (s, r)
}

pub fn bounded<T, E: Notify>(cap: usize) -> (Sender<T, E>, Receiver<T, E>) {
    assert!(cap > 0, "capacity cannot be zero");
    with_queue(ConcurrentQueue::bounded(cap))
}

impl<T, E: Notify> Sender<T, E> {
    pub fn try_send(&self, msg: T) -> Result<(), PushError<T>> {
        match self.channel.queue.push(msg) {
            Ok(()) => {
                self.channel.recv_ops.notify_additional_one();
                self.channel.stream_ops.notify_all();
                Ok(())
            }
            Err(err) => Err(err),
        }
    }
}

impl<T, E: Notify> Receiver<T, E> {
    pub fn try_recv(&self) -> Result<T, PopError> {
        match self.channel.queue.pop() {
            Ok(msg) => {
                self.channel.send_ops.notify_additional_one();
                Ok(msg)
            }
            Err(err) => Err(err),
        }
    }
}
