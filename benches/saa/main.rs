#[allow(dead_code)]
#[path = "../../examples/semaphore.rs"]
mod maillon_semaphore;

use criterion::{Criterion, criterion_group, criterion_main};
use saa::Semaphore;

fn acquire_release(c: &mut Criterion) {
    c.bench_function("Semaphore: acquire-release", |b| {
        b.iter(|| {
            let semaphore = Semaphore::default();
            semaphore.acquire_sync();
            assert!(semaphore.release());
        });
    });
}

fn acquire_acquire_release_release(c: &mut Criterion) {
    c.bench_function("Semaphore: acquire-acquire-release-release", |b| {
        b.iter(|| {
            let semaphore = Semaphore::default();
            semaphore.acquire_sync();
            semaphore.acquire_sync();
            assert!(semaphore.release());
            assert!(semaphore.release());
        });
    });
}

fn acquire_many_release_many(c: &mut Criterion) {
    c.bench_function("Semaphore: acquire-many-release-many", |b| {
        b.iter(|| {
            let semaphore = Semaphore::default();
            semaphore.acquire_many_sync(11);
            assert!(semaphore.release_many(11));
        });
    });
}

fn held_acquire_release(c: &mut Criterion) {
    c.bench_function("Semaphore held: acquire-release", |b| {
        let semaphore = Semaphore::default();
        semaphore.acquire_sync();
        b.iter(|| {
            semaphore.acquire_sync();
            assert!(semaphore.release());
        });
    });
}

fn held_acquire_acquire_release_release(c: &mut Criterion) {
    c.bench_function("Semaphore held: acquire-acquire-release-release", |b| {
        let semaphore = Semaphore::default();
        semaphore.acquire_sync();
        b.iter(|| {
            semaphore.acquire_sync();
            semaphore.acquire_sync();
            assert!(semaphore.release());
            assert!(semaphore.release());
        });
    });
}

fn held_acquire_many_release_many(c: &mut Criterion) {
    c.bench_function("Semaphore held: acquire-many-release-many", |b| {
        let semaphore = Semaphore::default();
        semaphore.acquire_sync();
        b.iter(|| {
            semaphore.acquire_many_sync(11);
            assert!(semaphore.release_many(11));
        });
    });
}

fn maillon_semaphore() -> maillon_semaphore::Semaphore {
    maillon_semaphore::Semaphore::new(Semaphore::MAX_PERMITS)
}

fn maillon_acquire_release(c: &mut Criterion) {
    c.bench_function("maillon Semaphore: acquire-release", |b| {
        b.iter(|| {
            let semaphore = maillon_semaphore();
            let permit = semaphore.try_acquire().unwrap();
            drop(permit);
        });
    });
}

fn maillon_acquire_acquire_release_release(c: &mut Criterion) {
    c.bench_function("maillon Semaphore: acquire-acquire-release-release", |b| {
        b.iter(|| {
            let semaphore = maillon_semaphore();
            let permit1 = semaphore.try_acquire().unwrap();
            let permit2 = semaphore.try_acquire().unwrap();
            drop(permit1);
            drop(permit2);
        });
    });
}

fn maillon_acquire_many_release_many(c: &mut Criterion) {
    c.bench_function("maillon Semaphore: acquire-many-release-many", |b| {
        b.iter(|| {
            let semaphore = maillon_semaphore();
            let permit = semaphore.try_acquire_many(11).unwrap();
            drop(permit);
        });
    });
}

fn maillon_held_acquire_release(c: &mut Criterion) {
    c.bench_function("maillon Semaphore held: acquire-release", |b| {
        let semaphore = maillon_semaphore();
        let _held = semaphore.try_acquire().unwrap();
        b.iter(|| {
            let permit = semaphore.try_acquire().unwrap();
            drop(permit);
        });
    });
}

fn maillon_held_acquire_acquire_release_release(c: &mut Criterion) {
    c.bench_function(
        "maillon Semaphore held: acquire-acquire-release-release",
        |b| {
            let semaphore = maillon_semaphore();
            let _held = semaphore.try_acquire().unwrap();
            b.iter(|| {
                let permit1 = semaphore.try_acquire().unwrap();
                let permit2 = semaphore.try_acquire().unwrap();
                drop(permit1);
                drop(permit2);
            });
        },
    );
}

fn maillon_held_acquire_many_release_many(c: &mut Criterion) {
    c.bench_function("maillon Semaphore held: acquire-many-release-many", |b| {
        let semaphore = maillon_semaphore();
        let _held = semaphore.try_acquire().unwrap();
        b.iter(|| {
            let permit = semaphore.try_acquire_many(11).unwrap();
            drop(permit);
        });
    });
}

criterion_group!(
    semaphore,
    acquire_release,
    acquire_acquire_release_release,
    acquire_many_release_many,
    held_acquire_release,
    held_acquire_acquire_release_release,
    held_acquire_many_release_many,
    maillon_acquire_release,
    maillon_acquire_acquire_release_release,
    maillon_acquire_many_release_many,
    maillon_held_acquire_release,
    maillon_held_acquire_acquire_release_release,
    maillon_held_acquire_many_release_many,
);
criterion_main!(semaphore);
