# maillon

A concurrent intrusive list with lock-free insertion, mainly for building synchronization primitives.

*Maillon is the French word for a chain link.*

## Features

- 100% safe API
- `#![no_std]`, no allocation
- Atomic emptiness check to avoid acquiring the mutex if the list is empty
- Lock-free[^1] insertion: multiple nodes can be inserted concurrently while another is being removed; removal requires locking
- Optional atomic state embedded in the list when empty (to carry a semaphore counter, a closed flag, etc.)
- `WaitList`, a high-level asynchronous wait list with customizable synchronization built on top of the low-level `List`

## Usage

`WaitList` is a ready-to-use asynchronous wait list, built on top of `List`:

```rust
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

use maillon::WaitList;

#[derive(Default)]
pub struct Event {
    done: AtomicBool,
    wait_list: WaitList,
}

impl Event {
    pub async fn wait(&self) {
        let _ = self.wait_list.wait_until(|_| self.done.load(Relaxed)).await;
    }

    pub fn set(&self) {
        self.done.store(true, Relaxed);
        self.wait_list.notify_all();
    }
}
```

`List` is the building block: `Node`s are pinned, carry user data implementing `NodeData`, and are pushed to the back without locking, while every other operation goes through `List::lock`. Here is a minimal wait list, supporting only `notify_one`:

```rust
use std::{
    future::Future,
    mem,
    pin::Pin,
    sync::atomic::Ordering::{Acquire, Release},
    task::{Context, Poll, Waker},
};

use maillon::{List, Node, NodeData, NodeState, list::LockedList, node_wrapper};

#[derive(Default)]
pub struct WaitList {
    list: List<Waiter>,
}

#[derive(Default)]
struct Waiter {
    waker: Option<Waker>,
    notified: bool,
}

impl WaitList {
    pub fn notify_one(&self) {
        if !self.list.is_empty_rmw(Release) {
            Self::notify_one_cold(&self.list);
        }
    }

    #[cold]
    fn notify_one_cold(list: &List<Waiter>) {
        Self::notify_one_locked(list.lock());
    }

    fn notify_one_locked(mut locked: LockedList<'_, Waiter>) {
        let Some(mut front) = locked.front() else {
            return;
        };
        front.notified = true;
        let waker = front.waker.take();
        front.unlink();
        drop(locked);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    pub fn wait(&self) -> Wait<'_> {
        Wait(Node::new(&self.list))
    }
}

node_wrapper! {
    pub struct Wait<'a>(Node<&'a List<Waiter>>);
}

impl Future for Wait<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        match self.node_mut().state() {
            NodeState::Unlinked(mut node) => {
                if mem::take(&mut node.notified) {
                    return Poll::Ready(());
                }
                node.waker = Some(cx.waker().clone());
                node.push_back(Acquire);
            }
            NodeState::Linked(mut node) => {
                if node.waker.as_ref().is_none_or(|w| !w.will_wake(cx.waker())) {
                    node.waker = Some(cx.waker().clone());
                }
            }
        }
        Poll::Pending
    }
}

impl NodeData<&List<Waiter>> for Waiter {
    fn new_state_if_last_node_on_drop(
        self: Pin<&mut Self>,
        _list: &&List<Waiter>,
        _list_data: &mut (),
    ) {
    }

    fn on_drop<'list>(
        self: Pin<&mut Self>,
        list: &'list &List<Waiter>,
        locked: Option<LockedList<'list, Self>>,
        _state_updated_on_unlink: bool,
    ) {
        if self.notified {
            match locked {
                Some(locked) => WaitList::notify_one_locked(locked),
                None => WaitList::notify_one_cold(list),
            }
        }
    }
}
```

See [examples](examples) for full implementations of `tokio::sync::Notify` and `tokio::sync::Semaphore` built with `maillon`, with fully identical API and behavior.

## Cargo Features

| Feature | Description |
|---------|-------------|
| `std` *(default)* | `std::sync`-based mutex and condvar parker |
| `atomic-wait` | Futex-based parker via the `atomic-wait` crate |
| `lock_api` | `lock_api::RawMutex` trait implementation |
| `parking_lot` | `parking_lot` mutex; implies `lock_api` |
| `portable-atomic` | Atomics via the `portable-atomic` crate, for targets without native atomic support |
| `pthread` | Raw pthread mutex and condition variable, on Unix targets only |

Without any features enabled, the library falls back to spin-based mutex and parker.

## Performance

Results of the `tokio` benchmarks, run with both `tokio` native primitives and their `maillon` counterparts from [examples](examples) on an Intel i7-1065G7:

*benchmarks prefixed by `contention`/`uncontented`[^2] measure `Semaphore` performance*

| Benchmark                       |       maillon |     tokio | maillon speedup |
|---------------------------------|----------:|----------:|------------:|
| `notify_one/10`                 | 200.35 µs | 247.28 µs |        1.23 |
| `notify_one/50`                 | 252.83 µs | 272.73 µs |        1.08 |
| `notify_one/100`                | 245.82 µs | 276.25 µs |        1.12 |
| `notify_one/200`                | 245.36 µs | 291.20 µs |        1.19 |
| `notify_one/500`                | 245.54 µs | 281.86 µs |        1.15 |
|                                 |           |           |             |
| `notify_waiters/10`             | 245.75 µs | 410.82 µs |        1.67 |
| `notify_waiters/50`             | 215.92 µs | 281.20 µs |        1.30 |
| `notify_waiters/100`            | 210.31 µs | 259.56 µs |        1.23 |
| `notify_waiters/200`            | 212.58 µs | 247.82 µs |        1.17 |
| `notify_waiters/500`            | 355.55 µs | 254.90 µs |        0.72 |
|                                 |           |           |             |
| `contention/concurrent_multi`   |   7.90 µs |   8.53 µs |        1.08 |
| `contention/concurrent_single`  | 500.41 ns | 679.58 ns |        1.36 |
|                                 |           |           |             |
| `uncontented/concurrent_multi`  |   9.02 µs |   9.09 µs |        1.01 |
| `uncontented/concurrent_single` | 529.70 ns | 624.12 ns |        1.18 |
| `uncontented/multi`             | 287.34 ns | 400.76 ns |        1.39 |

`maillon`-based reimplementations seem to give a consistent speedup compared to `tokio` native ones. The only exception is `notify_waiters/500`, and it can be explained by several factors:
- The benchmark results are extremely noisy, ranging from 200 µs to 400 µs, so `maillon` can in fact perform better than `tokio` on some runs.
- The scenario is not very realistic: all the threads are hammering the same cache line with CAS loops to requeue or notify in tight loops. The key point is that `maillon` doesn't use backoff in CAS loops, so they run in full-contention mode, while `tokio`'s native implementation serializes all operations. Adding exponential backoff to the `push_back` operation improves the result down to 150 µs.
- CPU hyperthreading typically handles this kind of ultra-contended scenario badly. Pinning the process to 4 cores only, or reducing the number of worker threads to 3 in order to avoid hyperthreading also greatly improves the result. Combined with exponential backoff, time drops below 100 µs.

## Safety and testing

Concurrent intrusive lists are one of the most unsafe[^3] concepts in Rust, so this crate uses unsafe code. It is tested with both [`miri`](https://github.com/rust-lang/miri/) and [`loom`](https://github.com/tokio-rs/loom) to ensure algorithm correctness and memory safety.

Reimplementations of `tokio::sync::Notify` and `tokio::sync::Semaphore` are also tested on the full tokio test suite (also with `miri` and `loom`).

`List` exposes a 100% safe API, so `WaitList` and `tokio` reimplementations don't use unsafe code[^4].

## Acknowledgements

The `maillon::list::Drain` algorithm reuses the idea originally introduced to `tokio` by [Tymoteusz Wiśniewski](https://github.com/satakuma) in [tokio-rs/tokio#5458](https://github.com/tokio-rs/tokio/pull/5458): make the draining atomic by moving the list nodes into a temporary circular list.

A small improvement, motivated by API ergonomics, has been made: the circular chaining is deferred until the list lock actually needs to be released mid-drain.

## License

Licensed under either of

- [Apache License, Version 2.0](LICENSE-APACHE)
- [MIT license](LICENSE-MIT)

at your option.

[^1]: In some rare cases, an inserting thread might need to unpark a remover thread, making insertion not strictly lock-free. It is also possible to switch the list to lazy node linking, making the node insertion fully lock-free. A third option is serialized linking, where insertion requires locking but can then happen at any position through a cursor, not only at the back.
[^2]: The `uncontented` typo comes from the original `tokio` benchmark.
[^3]: There is literally a [hack](https://rust-lang.github.io/rfcs/3467-unsafe-pinned.html) in the compiler to support them.
[^4]: Except for the pin projection of `wait_list::wait::WaitUntil`, written directly to avoid depending on `pin-project-lite`.
