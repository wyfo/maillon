#![cfg(not(any(miri, loom)))]
#[allow(dead_code)]
#[path = "../examples/semaphore.rs"]
mod semaphore;

mod linking;

use std::sync::Arc;

use aiq::list::Linking;
use linking::{EAGER, LAZY, LinkingMode};
use rstest::rstest;
use semaphore::Semaphore;

#[rstest]
fn no_permits<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    // this should not panic
    Semaphore::<L>::new(0);
}

#[rstest]
fn try_acquire<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let sem = Semaphore::<L>::new(1);
    {
        let p1 = sem.try_acquire();
        assert!(p1.is_ok());
        let p2 = sem.try_acquire();
        assert!(p2.is_err());
    }
    let p3 = sem.try_acquire();
    assert!(p3.is_ok());
}

#[rstest]
#[tokio::test]
async fn acquire<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let sem = Arc::new(Semaphore::<L>::new(1));
    let p1 = sem.try_acquire().unwrap();
    let sem_clone = sem.clone();
    let j = tokio::spawn(async move {
        let _p2 = sem_clone.acquire().await;
    });
    drop(p1);
    j.await.unwrap();
}

#[rstest]
#[tokio::test]
async fn add_permits<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let sem = Arc::new(Semaphore::<L>::new(0));
    let sem_clone = sem.clone();
    let j = tokio::spawn(async move {
        let _p2 = sem_clone.acquire().await;
    });
    sem.add_permits(1);
    j.await.unwrap();
}

#[rstest]
fn forget<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let sem = Arc::new(Semaphore::<L>::new(1));
    {
        let p = sem.try_acquire().unwrap();
        assert_eq!(sem.available_permits(), 0);
        p.forget();
        assert_eq!(sem.available_permits(), 0);
    }
    assert_eq!(sem.available_permits(), 0);
    assert!(sem.try_acquire().is_err());
}

#[rstest]
fn merge<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let sem = Arc::new(Semaphore::<L>::new(3));
    {
        let mut p1 = sem.try_acquire().unwrap();
        assert_eq!(sem.available_permits(), 2);
        let p2 = sem.try_acquire_many(2).unwrap();
        assert_eq!(sem.available_permits(), 0);
        p1.merge(p2);
        assert_eq!(sem.available_permits(), 0);
    }
    assert_eq!(sem.available_permits(), 3);
}

#[rstest]
#[cfg(not(target_family = "wasm"))] // No stack unwinding on wasm targets
#[should_panic]
fn merge_unrelated_permits<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let sem1 = Arc::new(Semaphore::<L>::new(3));
    let sem2 = Arc::new(Semaphore::<L>::new(3));
    let mut p1 = sem1.try_acquire().unwrap();
    let p2 = sem2.try_acquire().unwrap();
    p1.merge(p2);
}

#[rstest]
fn split<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let sem = Semaphore::<L>::new(5);
    let mut p1 = sem.try_acquire_many(3).unwrap();
    assert_eq!(sem.available_permits(), 2);
    assert_eq!(p1.num_permits(), 3);
    let mut p2 = p1.split(1).unwrap();
    assert_eq!(sem.available_permits(), 2);
    assert_eq!(p1.num_permits(), 2);
    assert_eq!(p2.num_permits(), 1);
    let p3 = p1.split(0).unwrap();
    assert_eq!(p3.num_permits(), 0);
    drop(p1);
    assert_eq!(sem.available_permits(), 4);
    let p4 = p2.split(1).unwrap();
    assert_eq!(p2.num_permits(), 0);
    assert_eq!(p4.num_permits(), 1);
    assert!(p2.split(1).is_none());
    drop(p2);
    assert_eq!(sem.available_permits(), 4);
    drop(p3);
    assert_eq!(sem.available_permits(), 4);
    drop(p4);
    assert_eq!(sem.available_permits(), 5);
}

#[rstest]
#[tokio::test]
async fn stress_test<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let sem = Arc::new(Semaphore::<L>::new(5));
    let mut join_handles = Vec::new();
    for _ in 0..1000 {
        let sem_clone = sem.clone();
        join_handles.push(tokio::spawn(async move {
            let _p = sem_clone.acquire().await;
        }));
    }
    for j in join_handles {
        j.await.unwrap();
    }
    // there should be exactly 5 semaphores available now
    let _p1 = sem.try_acquire().unwrap();
    let _p2 = sem.try_acquire().unwrap();
    let _p3 = sem.try_acquire().unwrap();
    let _p4 = sem.try_acquire().unwrap();
    let _p5 = sem.try_acquire().unwrap();
    assert!(sem.try_acquire().is_err());
}

#[rstest]
fn add_max_amount_permits<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let s = Semaphore::<L>::new(0);
    s.add_permits(Semaphore::<L>::MAX_PERMITS);
    assert_eq!(s.available_permits(), Semaphore::<L>::MAX_PERMITS);
}

#[cfg(not(target_family = "wasm"))] // wasm currently doesn't support unwinding
#[rstest]
#[should_panic]
fn add_more_than_max_amount_permits1<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let s = Semaphore::<L>::new(1);
    s.add_permits(Semaphore::<L>::MAX_PERMITS);
}

#[cfg(not(target_family = "wasm"))] // wasm currently doesn't support unwinding
#[rstest]
#[should_panic]
fn add_more_than_max_amount_permits2<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let s = Semaphore::<L>::new(Semaphore::<L>::MAX_PERMITS - 1);
    s.add_permits(1);
    s.add_permits(1);
}

#[cfg(not(target_family = "wasm"))] // wasm currently doesn't support unwinding
#[rstest]
#[should_panic]
fn panic_when_exceeds_maxpermits<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let _ = Semaphore::<L>::new(Semaphore::<L>::MAX_PERMITS + 1);
}

#[rstest]
fn no_panic_at_maxpermits<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    let _ = Semaphore::<L>::new(Semaphore::<L>::MAX_PERMITS);
    let s = Semaphore::<L>::new(Semaphore::<L>::MAX_PERMITS - 1);
    s.add_permits(1);
}
