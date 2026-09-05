#![cfg(not(any(miri, loom)))]
#[allow(dead_code)]
#[path = "../examples/notify.rs"]
mod notify;

mod linking;

use aiq::list::Linking;
use linking::{EAGER, LAZY, LinkingMode};
use notify::Notify;
use rstest::rstest;
use tokio_test::{task::spawn, *};

#[allow(unused)]
trait AssertSend: Send + Sync {}
impl<L: Linking> AssertSend for Notify<L> {}

#[rstest]
fn notify_notified_one<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let notify = Notify::<L>::new();
    let mut notified = spawn(async { notify.notified().await });

    notify.notify_one();
    assert_ready!(notified.poll());
}

#[rstest]
fn notify_multi_notified_one<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let notify = Notify::<L>::new();
    let mut notified1 = spawn(async { notify.notified().await });
    let mut notified2 = spawn(async { notify.notified().await });

    // add two waiters into the list
    assert_pending!(notified1.poll());
    assert_pending!(notified2.poll());

    // should wakeup the first one
    notify.notify_one();
    assert_ready!(notified1.poll());
    assert_pending!(notified2.poll());
}

#[rstest]
fn notify_multi_notified_last<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let notify = Notify::<L>::new();
    let mut notified1 = spawn(async { notify.notified().await });
    let mut notified2 = spawn(async { notify.notified().await });

    // add two waiters into the list
    assert_pending!(notified1.poll());
    assert_pending!(notified2.poll());

    // should wakeup the last one
    notify.notify_last();
    assert_pending!(notified1.poll());
    assert_ready!(notified2.poll());
}

#[rstest]
fn notified_one_notify<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let notify = Notify::<L>::new();
    let mut notified = spawn(async { notify.notified().await });

    assert_pending!(notified.poll());

    notify.notify_one();
    assert!(notified.is_woken());
    assert_ready!(notified.poll());
}

#[rstest]
fn notified_multi_notify<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let notify = Notify::<L>::new();
    let mut notified1 = spawn(async { notify.notified().await });
    let mut notified2 = spawn(async { notify.notified().await });

    assert_pending!(notified1.poll());
    assert_pending!(notified2.poll());

    notify.notify_one();
    assert!(notified1.is_woken());
    assert!(!notified2.is_woken());

    assert_ready!(notified1.poll());
    assert_pending!(notified2.poll());
}

#[rstest]
fn notify_notified_multi<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let notify = Notify::<L>::new();

    notify.notify_one();

    let mut notified1 = spawn(async { notify.notified().await });
    let mut notified2 = spawn(async { notify.notified().await });

    assert_ready!(notified1.poll());
    assert_pending!(notified2.poll());

    notify.notify_one();

    assert!(notified2.is_woken());
    assert_ready!(notified2.poll());
}

#[rstest]
fn notified_drop_notified_notify<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let notify = Notify::<L>::new();
    let mut notified1 = spawn(async { notify.notified().await });
    let mut notified2 = spawn(async { notify.notified().await });

    assert_pending!(notified1.poll());

    drop(notified1);

    assert_pending!(notified2.poll());

    notify.notify_one();
    assert!(notified2.is_woken());
    assert_ready!(notified2.poll());
}

#[rstest]
fn notified_multi_notify_drop_one<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let notify = Notify::<L>::new();
    let mut notified1 = spawn(async { notify.notified().await });
    let mut notified2 = spawn(async { notify.notified().await });

    assert_pending!(notified1.poll());
    assert_pending!(notified2.poll());

    notify.notify_one();

    assert!(notified1.is_woken());
    assert!(!notified2.is_woken());

    drop(notified1);

    assert!(notified2.is_woken());
    assert_ready!(notified2.poll());
}

#[rstest]
fn notified_multi_notify_one_drop<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let notify = Notify::<L>::new();
    let mut notified1 = spawn(async { notify.notified().await });
    let mut notified2 = spawn(async { notify.notified().await });
    let mut notified3 = spawn(async { notify.notified().await });

    // add waiters by order of poll execution
    assert_pending!(notified1.poll());
    assert_pending!(notified2.poll());
    assert_pending!(notified3.poll());

    // by default fifo
    notify.notify_one();

    drop(notified1);

    // next waiter should be the one to be to woken up
    assert_ready!(notified2.poll());
    assert_pending!(notified3.poll());
}

#[rstest]
fn notified_multi_notify_last_drop<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let notify = Notify::<L>::new();
    let mut notified1 = spawn(async { notify.notified().await });
    let mut notified2 = spawn(async { notify.notified().await });
    let mut notified3 = spawn(async { notify.notified().await });

    // add waiters by order of poll execution
    assert_pending!(notified1.poll());
    assert_pending!(notified2.poll());
    assert_pending!(notified3.poll());

    notify.notify_last();

    drop(notified3);

    // latest waiter added should be the one to woken up
    assert_ready!(notified2.poll());
    assert_pending!(notified1.poll());
}

#[rstest]
fn notify_in_drop_after_wake<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    use std::{future::Future, sync::Arc};

    use futures::task::ArcWake;

    let notify = Arc::new(Notify::<L>::new());

    struct NotifyOnDrop<L: Linking>(Arc<Notify<L>>);

    impl<L: Linking> ArcWake for NotifyOnDrop<L> {
        fn wake_by_ref(_arc_self: &Arc<Self>) {}
    }

    impl<L: Linking> Drop for NotifyOnDrop<L> {
        fn drop(&mut self) {
            self.0.notify_waiters();
        }
    }

    let mut fut = Box::pin(async {
        notify.notified().await;
    });

    {
        let waker = futures::task::waker(Arc::new(NotifyOnDrop(notify.clone())));
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(fut.as_mut().poll(&mut cx).is_pending());
    }

    // Now, notifying **should not** deadlock
    notify.notify_waiters();
}

#[rstest]
fn notify_one_after_dropped_all<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let notify = Notify::<L>::new();
    let mut notified1 = spawn(async { notify.notified().await });

    assert_pending!(notified1.poll());

    notify.notify_waiters();
    notify.notify_one();

    drop(notified1);

    let mut notified2 = spawn(async { notify.notified().await });

    assert_ready!(notified2.poll());
}

#[rstest]
fn test_notify_one_not_enabled<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let notify = Notify::<L>::new();
    let mut future = spawn(notify.notified());

    notify.notify_one();
    assert_ready!(future.poll());
}

#[rstest]
fn test_notify_one_after_enable<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let notify = Notify::<L>::new();
    let mut future = spawn(notify.notified());

    future.enter(|_, fut| assert!(!fut.enable()));

    notify.notify_one();
    assert_ready!(future.poll());
    future.enter(|_, fut| assert!(fut.enable()));
}

#[rstest]
fn test_poll_after_enable<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let notify = Notify::<L>::new();
    let mut future = spawn(notify.notified());

    future.enter(|_, fut| assert!(!fut.enable()));
    assert_pending!(future.poll());
}

#[rstest]
fn test_enable_after_poll<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let notify = Notify::<L>::new();
    let mut future = spawn(notify.notified());

    assert_pending!(future.poll());
    future.enter(|_, fut| assert!(!fut.enable()));
}

#[rstest]
fn test_enable_consumes_permit<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let notify = Notify::<L>::new();

    // Add a permit.
    notify.notify_one();

    let mut future1 = spawn(notify.notified());
    future1.enter(|_, fut| assert!(fut.enable()));

    let mut future2 = spawn(notify.notified());
    future2.enter(|_, fut| assert!(!fut.enable()));
}

#[rstest]
fn test_waker_update<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    use std::{future::Future, task::Context};

    use futures::task::noop_waker;

    let notify = Notify::<L>::new();
    let mut future = spawn(notify.notified());

    let noop = noop_waker();
    future.enter(|_, fut| assert_pending!(fut.poll(&mut Context::from_waker(&noop))));

    assert_pending!(future.poll());
    notify.notify_one();

    assert!(future.is_woken());
}
