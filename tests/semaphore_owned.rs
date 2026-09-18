#![cfg(not(any(miri, loom)))]
#[allow(dead_code)]
#[path = "../examples/semaphore.rs"]
mod semaphore;

mod linking;

use std::sync::Arc;

use linking::{EAGER, LAZY, LinkingMode, SERIALIZED};
use maillon::linking::Linking;
use rstest::rstest;
use semaphore::Semaphore;

#[rstest]
fn try_acquire<L: Linking>(#[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>) {
    let sem = Arc::new(Semaphore::<L>::new(1));
    {
        let p1 = sem.clone().try_acquire_owned();
        assert!(p1.is_ok());
        let p2 = sem.clone().try_acquire_owned();
        assert!(p2.is_err());
    }
    let p3 = sem.try_acquire_owned();
    assert!(p3.is_ok());
}

#[rstest]
fn try_acquire_many<L: Linking>(#[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>) {
    let sem = Arc::new(Semaphore::<L>::new(42));
    {
        let p1 = sem.clone().try_acquire_many_owned(42);
        assert!(p1.is_ok());
        let p2 = sem.clone().try_acquire_owned();
        assert!(p2.is_err());
    }
    let p3 = sem.clone().try_acquire_many_owned(32);
    assert!(p3.is_ok());
    let p4 = sem.clone().try_acquire_many_owned(10);
    assert!(p4.is_ok());
    assert!(sem.try_acquire_owned().is_err());
}

#[rstest]
#[tokio::test]
async fn acquire<L: Linking>(#[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>) {
    let sem = Arc::new(Semaphore::<L>::new(1));
    let p1 = sem.clone().try_acquire_owned().unwrap();
    let sem_clone = sem.clone();
    let j = tokio::spawn(async move {
        let _p2 = sem_clone.acquire_owned().await;
    });
    drop(p1);
    j.await.unwrap();
}

#[rstest]
#[tokio::test]
async fn acquire_many<L: Linking>(#[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>) {
    let semaphore = Arc::new(Semaphore::<L>::new(42));
    let permit32 = semaphore.clone().try_acquire_many_owned(32).unwrap();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let join_handle = tokio::spawn(async move {
        let _permit10 = semaphore.clone().acquire_many_owned(10).await.unwrap();
        sender.send(()).unwrap();
        let _permit32 = semaphore.acquire_many_owned(32).await.unwrap();
    });
    receiver.await.unwrap();
    drop(permit32);
    join_handle.await.unwrap();
}

#[rstest]
#[tokio::test]
async fn add_permits<L: Linking>(#[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>) {
    let sem = Arc::new(Semaphore::<L>::new(0));
    let sem_clone = sem.clone();
    let j = tokio::spawn(async move {
        let _p2 = sem_clone.acquire_owned().await;
    });
    sem.add_permits(1);
    j.await.unwrap();
}

#[rstest]
fn forget<L: Linking>(#[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>) {
    let sem = Arc::new(Semaphore::<L>::new(1));
    {
        let p = sem.clone().try_acquire_owned().unwrap();
        assert_eq!(sem.available_permits(), 0);
        p.forget();
        assert_eq!(sem.available_permits(), 0);
    }
    assert_eq!(sem.available_permits(), 0);
    assert!(sem.try_acquire_owned().is_err());
}

#[rstest]
fn merge<L: Linking>(#[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>) {
    let sem = Arc::new(Semaphore::<L>::new(3));
    {
        let mut p1 = sem.clone().try_acquire_owned().unwrap();
        assert_eq!(sem.available_permits(), 2);
        let p2 = sem.clone().try_acquire_many_owned(2).unwrap();
        assert_eq!(sem.available_permits(), 0);
        p1.merge(p2);
        assert_eq!(sem.available_permits(), 0);
    }
    assert_eq!(sem.available_permits(), 3);
}

#[rstest]
#[cfg(not(target_family = "wasm"))] // No stack unwinding on wasm targets
#[should_panic]
fn merge_unrelated_permits<L: Linking>(
    #[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>,
) {
    let sem1 = Arc::new(Semaphore::<L>::new(3));
    let sem2 = Arc::new(Semaphore::<L>::new(3));
    let mut p1 = sem1.try_acquire_owned().unwrap();
    let p2 = sem2.try_acquire_owned().unwrap();
    p1.merge(p2);
}

#[rstest]
fn split<L: Linking>(#[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>) {
    let sem = Arc::new(Semaphore::<L>::new(5));
    let mut p1 = sem.clone().try_acquire_many_owned(3).unwrap();
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
async fn stress_test<L: Linking>(#[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>) {
    let sem = Arc::new(Semaphore::<L>::new(5));
    let mut join_handles = Vec::new();
    for _ in 0..1000 {
        let sem_clone = sem.clone();
        join_handles.push(tokio::spawn(async move {
            let _p = sem_clone.acquire_owned().await;
        }));
    }
    for j in join_handles {
        j.await.unwrap();
    }
    // there should be exactly 5 semaphores available now
    let _p1 = sem.clone().try_acquire_owned().unwrap();
    let _p2 = sem.clone().try_acquire_owned().unwrap();
    let _p3 = sem.clone().try_acquire_owned().unwrap();
    let _p4 = sem.clone().try_acquire_owned().unwrap();
    let _p5 = sem.clone().try_acquire_owned().unwrap();
    assert!(sem.try_acquire_owned().is_err());
}
