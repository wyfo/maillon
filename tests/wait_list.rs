#[cfg(all(loom, not(debug_assertions)))]
compile_error!("loom tests requires debug_assertions enabled");

use std::{
    marker::PhantomData,
    pin::Pin,
    sync::atomic::Ordering::{Acquire, Relaxed, Release, SeqCst},
    task::{Context, Poll, Waker},
};
#[cfg(not(loom))]
use std::{
    sync::atomic::{AtomicUsize, fence},
    thread,
};

use aiq::{
    WaitList,
    list::Linking,
    wait_list::{
        ClosedError,
        synchronization::{Sequential, Synchronization, Synchronized, Unsynchronized},
        wait::WakeCondition,
    },
};
#[cfg(not(loom))]
use futures::executor::block_on;
use linking::{EAGER, LAZY, LinkingMode};
#[cfg(loom)]
use loom::{
    future::block_on,
    model,
    sync::atomic::{AtomicUsize, fence},
};
use rstest::rstest;

mod linking;

#[cfg(loom)]
mod thread {
    use std::{
        cell::RefCell,
        marker::PhantomData,
        panic::{AssertUnwindSafe, catch_unwind},
    };

    use loom::thread::JoinHandle;
    pub use loom::thread::spawn;

    #[derive(Default)]
    pub struct Scope<'env> {
        handles: RefCell<Vec<Option<JoinHandle<std::thread::Result<()>>>>>,
        dummy: loom::sync::Arc<loom::sync::atomic::AtomicUsize>,
        _env: PhantomData<&'env mut ()>,
    }

    impl Drop for Scope<'_> {
        fn drop(&mut self) {
            for handle in self.handles.get_mut().drain(..).flatten() {
                if let Err(err) = handle.join().unwrap() {
                    std::panic::resume_unwind(err);
                }
            }
        }
    }

    impl<'env> Scope<'env> {
        pub fn spawn<T: Send + 'env>(
            &self,
            f: impl FnOnce() -> T + Send + 'env,
        ) -> ScopedJoinHandle<'_, 'env> {
            let mut handles = self.handles.borrow_mut();
            let handle_idx = handles.len();
            let dummy = self.dummy.clone();
            handles.push(Some(spawn(unsafe {
                core::mem::transmute::<
                    Box<dyn FnOnce() -> std::thread::Result<()> + Send + 'env>,
                    Box<dyn FnOnce() -> std::thread::Result<()> + Send + 'static>,
                >(Box::new(move || {
                    // https://github.com/tokio-rs/loom/issues/392
                    dummy.store(1, loom::sync::atomic::Ordering::Relaxed);
                    // https://github.com/tokio-rs/loom/issues/417
                    catch_unwind(AssertUnwindSafe(|| {
                        f();
                    }))
                }))
            })));
            ScopedJoinHandle {
                scope: self,
                handle_idx,
            }
        }
    }

    pub struct ScopedJoinHandle<'a, 'env> {
        scope: &'a Scope<'env>,
        handle_idx: usize,
    }

    impl ScopedJoinHandle<'_, '_> {
        pub fn join(self) -> std::thread::Result<()> {
            self.scope.handles.borrow_mut()[self.handle_idx]
                .take()
                .unwrap()
                .join()
                .unwrap()
        }
    }

    pub fn scope<'env, T>(f: impl FnOnce(&Scope<'env>) -> T) -> T {
        let scope = Scope::default();
        scope.dummy.store(1, loom::sync::atomic::Ordering::Relaxed);
        f(&scope)
    }
}

#[cfg(not(loom))]
fn model(f: impl Fn() + Sync + Send + 'static) {
    f();
}

const WAKE_LIST_SIZE: usize = 32;

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
    Fifo,
    Lifo,
    All,
}

#[derive(Clone, Copy)]
enum WaitMode {
    Normal,
    Minimal,
}

trait WaitListExt<S: Synchronization, L: Linking> {
    fn notify(&self, mode: NotifyMode);
    #[expect(dead_code)]
    fn notify_many(&self, mode: NotifyMode, count: usize);
    async fn wait_until2<F: FnMut(bool) -> W, W: WakeCondition + Default>(
        &self,
        mode: WaitMode,
        wake_condition: F,
    ) -> Result<W::Output, ClosedError>;
}

impl<S: Synchronization, L: Linking> WaitListExt<S, L> for WaitList<S, L> {
    fn notify(&self, mode: NotifyMode) {
        match mode {
            NotifyMode::Fifo => self.notify_fifo(1),
            NotifyMode::Lifo => self.notify_lifo(1),
            NotifyMode::All => self.notify_all(),
        }
    }
    fn notify_many(&self, mode: NotifyMode, count: usize) {
        match mode {
            NotifyMode::Fifo => self.notify_fifo(count),
            NotifyMode::Lifo => self.notify_lifo(count),
            NotifyMode::All => unreachable!(),
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

trait FutureExt {
    type Output;
    fn poll_no_cx(self: Pin<&mut Self>) -> Poll<Self::Output>;
}

impl<F: Future> FutureExt for F {
    type Output = F::Output;
    fn poll_no_cx(self: Pin<&mut Self>) -> Poll<Self::Output> {
        self.poll(&mut Context::from_waker(Waker::noop()))
    }
}

macro_rules! assert_ready {
    ($e:expr) => {
        match $e.as_mut().poll_no_cx() {
            Poll::Ready(res) => res,
            Poll::Pending => panic!("future is pending"),
        }
    };
}

macro_rules! assert_pending {
    ($e:expr) => {
        if let Poll::Ready(value) = $e.as_mut().poll_no_cx() {
            panic!("future is ready: {value:?}");
        }
    };
}

#[rstest]
fn wait_until<S: Synchronization, L: Linking>(
    // loom doesn't support SEQ
    #[values(SYNC, UNSYNC, UNSYNC_RMW)] sync: SyncMode<S>,
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
    #[values(NotifyMode::Fifo, NotifyMode::Lifo, NotifyMode::All)] notify_mode: NotifyMode,
    #[values(WaitMode::Normal, WaitMode::Minimal)] wait_mode: WaitMode,
) where
    SyncMode<S>: WakeConditionAccess,
{
    model(move || {
        let list = WaitList::<S, L>::new();
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
fn notify_fifo_lifo<S: Synchronization, L: Linking>(
    #[values(SYNC, SEQ, UNSYNC)] _sync: SyncMode<S>,
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
    #[values((NotifyMode::Fifo, 0), (NotifyMode::Lifo, 1))] (notify_mode, wait_idx): (
        NotifyMode,
        usize,
    ),
) {
    model(move || {
        let list = WaitList::<S, L>::new();
        let mut waits = [Box::pin(list.wait()), Box::pin(list.wait())];
        assert_pending!(waits[0]);
        assert_pending!(waits[1]);
        list.notify(notify_mode);
        assert_ready!(waits[wait_idx]).unwrap();
        assert_pending!(waits[1 - wait_idx]);
    });
}

#[rstest]
fn notify_fifo_lifo_cancel<S: Synchronization, L: Linking>(
    #[values(SYNC, SEQ, UNSYNC)] _sync: SyncMode<S>,
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
    #[values((NotifyMode::Fifo, 0), (NotifyMode::Lifo, 1))] (notify_mode, wait_idx): (
        NotifyMode,
        usize,
    ),
) {
    model(move || {
        let list = WaitList::<S, L>::new();
        let mut notified = Box::pin(list.wait());
        assert_pending!(notified);
        list.notify(notify_mode);
        let mut waits = [Box::pin(list.wait()), Box::pin(list.wait())];
        assert_pending!(waits[0]);
        assert_pending!(waits[1]);
        drop(notified);
        assert_ready!(waits[wait_idx]).unwrap();
        assert_pending!(waits[1 - wait_idx]);
    });
}

#[rstest]
fn notify_all_cancel<S: Synchronization, L: Linking>(
    #[values(SYNC, SEQ, UNSYNC)] _sync: SyncMode<S>,
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
) {
    model(|| {
        let list = WaitList::<S, L>::new();
        let mut notified = Box::pin(list.wait());
        assert_pending!(notified);
        list.notify_all();
        let mut wait = Box::pin(list.wait());
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
        let list = WaitList::<S, L>::new();
        let mut wait1 = Box::pin(list.wait());
        let mut wait2 = Box::pin(list.wait());
        assert_pending!(wait1);
        assert_pending!(wait2);
        thread::scope(|s| {
            s.spawn(|| list.notify_all());
            s.spawn(|| {
                let res1 = wait1.as_mut().poll_no_cx();
                let res2 = wait2.as_mut().poll_no_cx();
                assert!(res1.is_pending() || res2.is_ready());
            });
        });
    });
}

#[rstest]
fn notify_all_is_atomic<S: Synchronization, L: Linking>(
    #[values(SYNC, SEQ, UNSYNC)] _sync: SyncMode<S>,
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
    #[values(0, WAKE_LIST_SIZE)] tested_fut_index: usize,
) {
    model(move || {
        let list = WaitList::<S, L>::new();
        let mut futs = (0..WAKE_LIST_SIZE + 1)
            .map(|_| Box::pin(list.wait()))
            .collect::<Vec<_>>();
        for fut in &mut futs {
            assert_pending!(fut);
        }
        thread::scope(|s| {
            s.spawn(|| list.notify_all());
            s.spawn(|| {
                block_on(async {
                    futs.remove(tested_fut_index).await.unwrap();
                    let mut new_fut = Box::pin(list.wait());
                    assert_pending!(new_fut);
                    list.notify_fifo(1);
                    assert_ready!(new_fut).unwrap();
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
        let list = WaitList::<S, L>::new();
        let mut wait = Box::pin(list.wait());
        assert_pending!(wait);
        thread::scope(|s| {
            s.spawn(|| list.notify_all());
            s.spawn(|| {
                block_on(async {
                    wait.await.unwrap();
                    assert_pending!(Box::pin(list.wait()));
                });
            });
        });
    });
}
