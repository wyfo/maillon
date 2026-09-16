#[allow(dead_code)]
#[path = "../examples/semaphore.rs"]
mod semaphore;

mod linking;

use std::{
    future::{Future, poll_fn},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
    task::Poll,
    thread,
};

use aiq::list::Linking;
use futures::executor::block_on;
use linking::{EAGER, LAZY, LinkingMode, SERIALIZED};
use rstest::rstest;
use semaphore::Semaphore;

#[rstest]
fn basic_usage<L: Linking>(#[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>) {
    const NUM: usize = 2;

    struct Shared<L: Linking> {
        semaphore: Semaphore<L>,
        active: AtomicUsize,
    }

    async fn actor<L: Linking>(shared: Arc<Shared<L>>) {
        let _permit = shared.semaphore.acquire().await.unwrap();
        let actual = shared.active.fetch_add(1, SeqCst);
        assert!(actual < NUM);

        let actual = shared.active.fetch_sub(1, SeqCst);
        assert!(actual <= NUM);
    }

    let shared = Arc::new(Shared {
        semaphore: Semaphore::<L>::new(NUM),
        active: AtomicUsize::new(0),
    });

    for _ in 0..NUM {
        let shared = shared.clone();

        thread::spawn(move || {
            block_on(actor(shared));
        });
    }

    block_on(actor(shared));
}

#[rstest]
fn release<L: Linking>(#[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>) {
    let semaphore = Arc::new(Semaphore::<L>::new(1));

    {
        let semaphore = semaphore.clone();
        thread::spawn(move || {
            block_on(semaphore.acquire()).unwrap();
        });
    }

    block_on(semaphore.acquire()).unwrap();
}

#[rstest]
fn basic_closing<L: Linking>(#[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>) {
    const NUM: usize = 2;

    let semaphore = Arc::new(Semaphore::<L>::new(1));

    for _ in 0..NUM {
        let semaphore = semaphore.clone();

        thread::spawn(move || {
            for _ in 0..2 {
                block_on(semaphore.acquire()).map_err(|_| ())?;
            }

            Ok::<(), ()>(())
        });
    }

    semaphore.close();
}

#[rstest]
fn concurrent_close<L: Linking>(#[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>) {
    const NUM: usize = 3;

    let semaphore = Arc::new(Semaphore::<L>::new(1));

    for _ in 0..NUM {
        let semaphore = semaphore.clone();

        thread::spawn(move || {
            block_on(semaphore.acquire()).map_err(|_| ())?;
            semaphore.close();

            Ok::<(), ()>(())
        });
    }
}

#[rstest]
fn concurrent_cancel<L: Linking>(#[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>) {
    async fn poll_and_cancel<L: Linking>(semaphore: Arc<Semaphore<L>>) {
        let mut acquire1 = Some(semaphore.acquire());
        let mut acquire2 = Some(semaphore.acquire());
        poll_fn(|cx| {
            // poll the acquire future once, and then immediately throw
            // it away. this simulates a situation where a future is
            // polled and then cancelled, such as by a timeout.
            if let Some(acquire) = acquire1.take() {
                tokio::pin!(acquire);
                let _ = acquire.poll(cx);
            }
            if let Some(acquire) = acquire2.take() {
                tokio::pin!(acquire);
                let _ = acquire.poll(cx);
            }
            Poll::Ready(())
        })
        .await;
    }

    let semaphore = Arc::new(Semaphore::<L>::new(0));
    let t1 = {
        let semaphore = semaphore.clone();
        thread::spawn(move || block_on(poll_and_cancel(semaphore)))
    };
    let t2 = {
        let semaphore = semaphore.clone();
        thread::spawn(move || block_on(poll_and_cancel(semaphore)))
    };
    let t3 = {
        let semaphore = semaphore.clone();
        thread::spawn(move || block_on(poll_and_cancel(semaphore)))
    };

    t1.join().unwrap();
    semaphore.add_permits(10);
    t2.join().unwrap();
    t3.join().unwrap();
}

#[rstest]
fn batch<L: Linking>(#[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>) {
    let semaphore = Arc::new(Semaphore::<L>::new(10));
    let active = Arc::new(AtomicUsize::new(0));
    let mut threads = vec![];

    for _ in 0..2 {
        let semaphore = semaphore.clone();
        let active = active.clone();

        threads.push(thread::spawn(move || {
            for n in &[4, 10, 8] {
                let _permits = block_on(semaphore.acquire_many(*n)).unwrap();

                active.fetch_add(*n as usize, SeqCst);

                let num_active = active.load(SeqCst);
                assert!(num_active <= 10);

                thread::yield_now();

                active.fetch_sub(*n as usize, SeqCst);
            }
        }));
    }

    for thread in threads.into_iter() {
        thread.join().unwrap();
    }

    assert_eq!(10, semaphore.available_permits());
}

#[rstest]
fn release_during_acquire<L: Linking>(#[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>) {
    let semaphore = Arc::new(Semaphore::<L>::new(10));
    let permits = semaphore
        .try_acquire_many(8)
        .expect("try_acquire should succeed; semaphore uncontended");
    let semaphore2 = semaphore.clone();
    let thread = thread::spawn(move || block_on(semaphore2.acquire_many(4)).unwrap().forget());

    drop(permits);
    thread.join().unwrap();
    semaphore.add_permits(4);
    assert_eq!(10, semaphore.available_permits());
}

#[rstest]
fn concurrent_permit_updates<L: Linking>(
    #[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>,
) {
    let semaphore = Arc::new(Semaphore::<L>::new(5));
    let t1 = {
        let semaphore = semaphore.clone();
        thread::spawn(move || semaphore.add_permits(3))
    };
    let t2 = {
        let semaphore = semaphore.clone();
        thread::spawn(move || {
            semaphore
                .try_acquire()
                .expect("try_acquire should succeed")
                .forget();
        })
    };
    let t3 = {
        let semaphore = semaphore.clone();
        thread::spawn(move || semaphore.forget_permits(2))
    };

    t1.join().unwrap();
    t2.join().unwrap();
    t3.join().unwrap();
    assert_eq!(semaphore.available_permits(), 5);
}
