use std::{
    marker::PhantomData,
    sync::atomic::Ordering::{Acquire, Relaxed, Release, SeqCst},
    task::{Context, Poll, Waker},
};

use aiq::{
    WaitList,
    list::Linking,
    wait_list::{
        ClosedError, DEFAULT_WAKER_LIST_SIZE,
        synchronization::{Sequential, Synchronization, Synchronized, Unsynchronized},
        wait::WakeCondition,
    },
};
use futures::FutureExt;
use linking::{EAGER, LAZY, LinkingMode};
use loom::{AtomicUsize, block_on, fence, model, thread};
use rstest::rstest;

mod linking;
mod loom;

struct SyncMode<S: Synchronization> {
    _sync: PhantomData<S>,
    rmw: bool,
}
impl<S: Synchronization> Copy for SyncMode<S> {}
impl<S: Synchronization> Clone for SyncMode<S> {
    fn clone(&self) -> Self {
        *self
    }
}

const SYNC: SyncMode<Synchronized> = SyncMode {
    _sync: PhantomData,
    rmw: false,
};
const SEQ: SyncMode<Sequential> = SyncMode {
    _sync: PhantomData,
    rmw: false,
};
const UNSYNC: SyncMode<Unsynchronized> = SyncMode {
    _sync: PhantomData,
    rmw: false,
};
const UNSYNC_RMW: SyncMode<Unsynchronized> = SyncMode {
    _sync: PhantomData,
    rmw: true,
};

trait WakeConditionAccess {
    fn set(self, c: &AtomicUsize, v: usize);
    fn get(self, c: &AtomicUsize, registered: bool) -> usize;
}
impl WakeConditionAccess for SyncMode<Synchronized> {
    fn set(self, c: &AtomicUsize, v: usize) {
        c.store(v, Relaxed);
    }
    fn get(self, c: &AtomicUsize, _registered: bool) -> usize {
        c.load(Relaxed)
    }
}
impl WakeConditionAccess for SyncMode<Sequential> {
    fn set(self, c: &AtomicUsize, v: usize) {
        c.store(v, SeqCst);
    }
    fn get(self, c: &AtomicUsize, registered: bool) -> usize {
        if registered {
            c.load(SeqCst)
        } else {
            c.load(Relaxed)
        }
    }
}
impl WakeConditionAccess for SyncMode<Unsynchronized> {
    fn set(self, c: &AtomicUsize, v: usize) {
        if self.rmw {
            c.swap(v, Acquire);
        } else {
            c.store(v, Relaxed);
            fence(SeqCst);
        }
    }
    fn get(self, c: &AtomicUsize, registered: bool) -> usize {
        if self.rmw {
            if registered {
                c.fetch_add(0, Release)
            } else {
                c.load(Relaxed)
            }
        } else {
            if registered {
                fence(SeqCst);
            }
            c.load(Relaxed)
        }
    }
}

#[derive(Clone, Copy)]
enum NotifyMode {
    One,
    Last,
    All,
}

#[derive(Clone, Copy)]
enum WaitMode {
    Normal,
    Minimal,
}

trait WaitListExt<S: Synchronization, L: Linking> {
    fn notify(&self, mode: NotifyMode);
    async fn wait_until2<F: FnMut(bool) -> W, W: WakeCondition + Default>(
        &self,
        mode: WaitMode,
        wake_condition: F,
    ) -> Result<W::Output, ClosedError>;
}

impl<S: Synchronization, L: Linking> WaitListExt<S, L> for WaitList<(), S, L> {
    fn notify(&self, mode: NotifyMode) {
        match mode {
            NotifyMode::One => self.notify_one(),
            NotifyMode::Last => self.notify_last(),
            NotifyMode::All => self.notify_all(),
        }
    }
    async fn wait_until2<F: FnMut(bool) -> W, W: WakeCondition + Default>(
        &self,
        mode: WaitMode,
        mut wake_condition: F,
    ) -> Result<W::Output, ClosedError> {
        let wake_condition = |registered: bool| match mode {
            WaitMode::Minimal if !registered => W::default(),
            _ => wake_condition(registered),
        };
        self.wait_until(wake_condition).await
    }
}

trait WaitListExt2<N: Unpin + Copy, S: Synchronization, L: Linking> {
    fn notify_with(&self, mode: NotifyMode, notif: N);
}

impl<N: Unpin + Copy, S: Synchronization, L: Linking> WaitListExt2<N, S, L> for WaitList<N, S, L> {
    fn notify_with(&self, mode: NotifyMode, notif: N) {
        match mode {
            NotifyMode::One => self.notify_one_with(|| notif),
            NotifyMode::Last => self.notify_last_with(|| notif),
            NotifyMode::All => self.notify_all_with(|| notif),
        }
    }
}

macro_rules! assert_ready {
    ($e:expr) => {
        match $e.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(res) => res,
            Poll::Pending => panic!("future is pending"),
        }
    };
    ($e:expr, $expect:expr) => {
        assert_eq!(assert_ready!($e), $expect);
    };
}

macro_rules! assert_pending {
    ($e:expr) => {
        if let Poll::Ready(value) = $e.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            panic!("future is ready: {value:?}");
        }
    };
}

#[rstest]
fn wait_until<S: Synchronization, L: Linking>(
    // loom doesn't support SEQ
    #[values(SYNC, UNSYNC, UNSYNC_RMW)] sync: SyncMode<S>,
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
    #[values(NotifyMode::One, NotifyMode::Last, NotifyMode::All)] notify_mode: NotifyMode,
    #[values(WaitMode::Normal, WaitMode::Minimal)] wait_mode: WaitMode,
) where
    SyncMode<S>: WakeConditionAccess,
{
    model(move || {
        let list = WaitList::<(), S, L>::new();
        let wake_condition = AtomicUsize::new(0);
        thread::scope(|s| {
            s.spawn(|| {
                sync.set(&wake_condition, 1);
                list.notify(notify_mode);
            });
            s.spawn(|| {
                block_on(list.wait_until2(wait_mode, |registered| {
                    sync.get(&wake_condition, registered) == 1
                }))
                .unwrap();
            });
        });
    });
}

#[rstest]
fn notify_one_last<S: Synchronization, L: Linking>(
    #[values(SYNC, SEQ, UNSYNC)] _sync: SyncMode<S>,
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
    #[values((NotifyMode::One, 0), (NotifyMode::Last, 1))] (notify_mode, wait_idx): (
        NotifyMode,
        usize,
    ),
) {
    model(move || {
        let list = WaitList::<usize, S, L>::new();
        let mut waits = [list.wait().boxed(), list.wait().boxed()];
        assert_pending!(waits[0]);
        assert_pending!(waits[1]);
        list.notify_with(notify_mode, 42);
        assert_ready!(waits[wait_idx], Ok(42));
        assert_pending!(waits[1 - wait_idx]);
    });
}

#[rstest]
fn notify_one_last_cancel<S: Synchronization, L: Linking>(
    #[values(SYNC, SEQ, UNSYNC)] _sync: SyncMode<S>,
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
    #[values((NotifyMode::One, 0), (NotifyMode::Last, 1))] (notify_mode, wait_idx): (
        NotifyMode,
        usize,
    ),
) {
    model(move || {
        let list = WaitList::<usize, S, L>::new();
        let mut notified = list.wait().boxed();
        assert_pending!(notified);
        list.notify_with(notify_mode, 42);
        let mut waits = [list.wait().boxed(), list.wait().boxed()];
        assert_pending!(waits[0]);
        assert_pending!(waits[1]);
        drop(notified);
        assert_ready!(waits[wait_idx], Ok(42));
        assert_pending!(waits[1 - wait_idx]);
    });
}

#[rstest]
fn notify_all_cancel<S: Synchronization, L: Linking>(
    #[values(SYNC, SEQ, UNSYNC)] _sync: SyncMode<S>,
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
) {
    model(|| {
        let list = WaitList::<(), S, L>::new();
        let mut notified = list.wait().boxed();
        assert_pending!(notified);
        list.notify_all();
        let mut wait = list.wait().boxed();
        assert_pending!(wait);
        drop(notified);
        assert_pending!(wait);
    });
}

#[rstest]
fn notify_all_poll_consistency<S: Synchronization, L: Linking>(
    #[values(SYNC, SEQ, UNSYNC)] _sync: SyncMode<S>,
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
) {
    model(|| {
        let list = WaitList::<(), S, L>::new();
        let mut wait1 = list.wait().boxed();
        let mut wait2 = list.wait().boxed();
        assert_pending!(wait1);
        assert_pending!(wait2);
        thread::scope(|s| {
            s.spawn(|| list.notify_all());
            s.spawn(|| {
                let res1 = wait1.now_or_never();
                let res2 = wait2.now_or_never();
                assert!(res1.is_none() || res2.is_some());
            });
        });
    });
}

#[rstest]
fn notify_all_is_atomic<S: Synchronization, L: Linking>(
    #[values(SYNC, SEQ, UNSYNC)] _sync: SyncMode<S>,
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
    #[values(0, DEFAULT_WAKER_LIST_SIZE)] tested_fut_index: usize,
) {
    model(move || {
        let list = WaitList::<&'static str, S, L>::new();
        let mut futs = (0..DEFAULT_WAKER_LIST_SIZE + 1)
            .map(|_| list.wait().boxed())
            .collect::<Vec<_>>();
        for fut in &mut futs {
            assert_pending!(fut);
        }
        thread::scope(|s| {
            s.spawn(|| list.notify_all_with(|| "all"));
            s.spawn(|| {
                block_on(async {
                    assert_eq!(futs.remove(tested_fut_index).await, Ok("all"));
                    let mut new_fut = list.wait().boxed();
                    assert_pending!(new_fut);
                    list.notify_one_with(|| "one");
                    assert_ready!(new_fut, Ok("one"));
                });
            });
        });
    });
}

#[rstest]
fn notify_all_sequential_wait<S: Synchronization, L: Linking>(
    #[values(SYNC, SEQ, UNSYNC)] _sync: SyncMode<S>,
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
) {
    model(move || {
        let list = WaitList::<(), S, L>::new();
        let mut wait = list.wait().boxed();
        assert_pending!(wait);
        thread::scope(|s| {
            s.spawn(|| list.notify_all());
            s.spawn(|| {
                block_on(async {
                    wait.await.unwrap();
                    assert_pending!(list.wait().boxed());
                });
            });
        });
    });
}

#[rstest]
fn notify_many<S: Synchronization, L: Linking>(
    #[values(SYNC, SEQ, UNSYNC)] _sync: SyncMode<S>,
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
    #[values(0, 2, 5)] count: usize,
) {
    model(move || {
        let list = WaitList::<usize, S, L>::new();
        let mut waits = (0..4).map(|_| list.wait().boxed()).collect::<Vec<_>>();
        for wait in &mut waits {
            assert_pending!(wait);
        }
        let mut i = 0;
        list.notify_many_with(count, || {
            let n = i;
            i += 1;
            n
        });
        let len = waits.len();
        let notified = count.min(len);
        for (i, wait) in waits.iter_mut().enumerate() {
            if i < notified {
                assert_ready!(wait, Ok(i));
            } else {
                assert_pending!(wait);
            }
        }
    });
}

#[rstest]
fn notify_many_cancel_race<S: Synchronization, L: Linking>(
    #[values(SYNC, SEQ, UNSYNC)] _sync: SyncMode<S>,
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
) {
    const COUNT: usize = DEFAULT_WAKER_LIST_SIZE + 1;
    model(move || {
        let list = WaitList::<(), S, L>::new();
        let mut waits = (0..DEFAULT_WAKER_LIST_SIZE + 4)
            .map(|_| list.wait().boxed())
            .collect::<Vec<_>>();
        for wait in &mut waits {
            assert_pending!(wait);
        }
        let cancelled = waits.remove(0);
        thread::scope(|s| {
            s.spawn(|| list.notify_many(COUNT));
            s.spawn(move || drop(cancelled));
        });
        for (i, wait) in waits.iter_mut().enumerate() {
            if i < COUNT {
                assert_ready!(wait).unwrap();
            } else {
                assert_pending!(wait);
            }
        }
    });
}

#[rstest]
fn close<S: Synchronization, L: Linking>(
    #[values(SYNC, SEQ, UNSYNC)] _sync: SyncMode<S>,
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
) {
    model(|| {
        let list = WaitList::<(), S, L>::new();
        assert!(!list.is_closed());
        let mut wait = list.wait().boxed();
        assert_pending!(wait);
        list.close();
        assert!(list.is_closed());
        assert!(list.is_empty());
        assert_eq!(assert_ready!(wait), Err(ClosedError));
        assert_eq!(assert_ready!(list.wait().boxed()), Err(ClosedError));
    });
}

#[rstest]
fn wait_until_closed<S: Synchronization, L: Linking>(
    #[values(SYNC, SEQ, UNSYNC)] _sync: SyncMode<S>,
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
) {
    model(|| {
        let list = WaitList::<(), S, L>::new();
        list.close();
        assert_eq!(assert_ready!(list.wait_until(|_| true).boxed()), Ok(()));
        assert_eq!(
            assert_ready!(list.wait_until(|_| false).boxed()),
            Err(ClosedError)
        );
    });
}

#[rstest]
fn wait_until_predicate_has_priority_on_close<S: Synchronization, L: Linking>(
    #[values(SYNC, SEQ, UNSYNC, UNSYNC_RMW)] sync: SyncMode<S>,
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
) where
    SyncMode<S>: WakeConditionAccess,
{
    model(move || {
        let list = WaitList::<(), S, L>::new();
        let value = AtomicUsize::new(0);
        thread::scope(|s| {
            s.spawn(|| {
                sync.set(&value, 1);
                list.close();
            });
            s.spawn(|| {
                let res = block_on(list.wait_until(|registered| sync.get(&value, registered) == 1));
                assert_eq!(res, Ok(()));
            });
        });
    });
}

#[rstest]
fn close_synchronization<S: Synchronization, L: Linking>(
    #[values(SYNC, SEQ, UNSYNC)] _sync: SyncMode<S>,
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
) {
    model(|| {
        let list = WaitList::<(), S, L>::new();
        let value = AtomicUsize::new(0);
        thread::scope(|s| {
            s.spawn(|| {
                value.store(1, Relaxed);
                list.close();
            });
            s.spawn(|| {
                block_on(list.wait()).unwrap_err();
                assert_eq!(value.load(Relaxed), 1);
            });
        });
    });
}

#[rstest]
fn notify_cancel_race<S: Synchronization, L: Linking>(
    #[values(SYNC, SEQ, UNSYNC)] _sync: SyncMode<S>,
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
) {
    model(|| {
        let list = WaitList::<usize, S, L>::new();
        let mut cancelled = list.wait().boxed();
        let mut wait = list.wait().boxed();
        let mut other = list.wait().boxed();
        assert_pending!(cancelled);
        assert_pending!(wait);
        assert_pending!(other);
        thread::scope(|s| {
            s.spawn(|| list.notify_one_with(|| 42));
            s.spawn(move || drop(cancelled));
        });
        assert_ready!(wait, Ok(42));
        assert_pending!(other);
    });
}

#[rstest]
fn wait_until_notified_completion<S: Synchronization, L: Linking>(
    #[values(SYNC, SEQ, UNSYNC)] _sync: SyncMode<S>,
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
) {
    model(|| {
        let list = WaitList::<(), S, L>::new();
        let cond = AtomicUsize::new(0);
        let mut wait = Box::pin(list.wait_until(|_| cond.load(Relaxed) == 1));
        assert_pending!(wait);
        let mut other = list.wait().boxed();
        assert_pending!(other);
        list.notify_one();
        assert_pending!(wait); // reinserted in last position
        list.notify_last();
        cond.store(1, Relaxed);
        assert_ready!(wait).unwrap();
        drop(wait);
        assert_pending!(other);
    });
}

#[rstest]
fn notify_all_cancel_during_drain<S: Synchronization, L: Linking>(
    #[values(SYNC, SEQ, UNSYNC)] _sync: SyncMode<S>,
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
    #[values(0, DEFAULT_WAKER_LIST_SIZE)] cancelled_index: usize,
) {
    model(move || {
        let list = WaitList::<(), S, L>::new();
        let mut futs = (0..DEFAULT_WAKER_LIST_SIZE + 2)
            .map(|_| list.wait().boxed())
            .collect::<Vec<_>>();
        for fut in &mut futs {
            assert_pending!(fut);
        }
        let cancelled = futs.remove(cancelled_index);
        thread::scope(|s| {
            s.spawn(|| list.notify_all());
            s.spawn(move || drop(cancelled));
        });
        for fut in &mut futs {
            assert_ready!(fut, Ok(()));
        }
    });
}

#[rstest]
fn notify_all_push_during_drain<S: Synchronization, L: Linking>(
    #[values(SYNC, SEQ, UNSYNC)] _sync: SyncMode<S>,
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
) {
    model(|| {
        let list = WaitList::<(), S, L>::new();
        let mut wait = list.wait().boxed();
        assert_pending!(wait);
        thread::scope(|s| {
            s.spawn(|| list.notify_all());
            s.spawn(|| {
                let mut pushed = list.wait().boxed();
                assert_pending!(pushed);
                drop(pushed);
            });
        });
        assert_ready!(wait, Ok(()));
    });
}
