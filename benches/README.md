# Benchmarks

* /!\ WARNING: This file is LLM-generated and has not been reworded/!\*

All results: Intel i7-1065G7, Windows 11, on AC power. Each implementation is run in its own
invocation: on this laptop, sustained runs slow down after a few seconds, which biases whatever
runs last. Numbers are medians. The `tokio` comparison is in the [crate README](../README.md#performance).

## asyncband

`cargo bench --bench asyncband -- <module path> --min-time 0.1` (divan).

`asyncband/` holds asyncband's own `event` and `semaphore` benchmarks, copied from upstream;
`maillon/` and `async_event/` run the same files against `examples/semaphore.rs` and against
`ManualResetEvent` reimplementations. `fulfill_debt_repeatedly` is not ported: tokio-style
`forget_permits` has no permit debt.

### Semaphore

| Benchmark                       | asyncband |  maillon | maillon speedup |
|---------------------------------|----------:|---------:|----------------:|
| `cancel_pending_acquire`        |   90.4 ns |  49.7 ns |            1.82 |
| `cancel_pending_owned_batch/1`  |  136.4 ns | 130.9 ns |            1.04 |
| `cancel_pending_owned_batch/8`  |    925 ns |   762 ns |            1.21 |
| `cancel_pending_owned_batch/32` |   4.00 µs |  3.00 µs |            1.33 |
| `handoff_permit`                |  120.8 ns |  60.3 ns |            2.00 |
| `owned_try_acquire_rejected`    |   12.2 ns |  10.4 ns |            1.18 |
| `owned_try_acquire_release`     |   36.4 ns |  24.7 ns |            1.47 |
| `queued_owned_burst/1`          |    293 ns |   214 ns |            1.37 |
| `queued_owned_burst/8`          |   1.27 µs |   950 ns |            1.34 |
| `queued_owned_burst/32`         |   4.80 µs |  3.45 µs |            1.39 |
| `try_acquire_release`           |   27.9 ns |  20.6 ns |            1.35 |
| `release`                       |   28.4 ns |  12.2 ns |            2.33 |
| `release_to_waiters/1`          |   27.9 ns |  34.1 ns |            0.82 |
| `release_to_waiters/2`          |   35.7 ns |  45.0 ns |            0.79 |
| `release_to_waiters/32`         |    439 ns |   459 ns |            0.96 |
| `release_to_waiters/256`        |   4.29 µs |  6.03 µs |            0.71 |

`release_to_waiters` only times the release, not the waiter registration. With one or two
waiters, maillon pays an extra tail CAS to restore the state into the tail word. With more, divan
builds all inputs before timing, so maillon's waiter nodes, one per boxed future, are cold, while
asyncband's are in a contiguous arena. With `--sample-size 1`, which keeps them hot, maillon is
faster: 400 ns vs 500 ns at 32 waiters, 3.00 µs vs 3.60 µs at 256.

### Event

| Benchmark               | asyncband | async-event | maillon `WaitList` | maillon `List` |
|-------------------------|----------:|------------:|-------------------:|---------------:|
| `cancel_pending`        |   70.0 ns |     79.4 ns |            52.4 ns |        41.5 ns |
| `is_set_contended` t=8  |    794 ns |      < 2 ns |             < 2 ns |         < 2 ns |
| `set_reset_cycle`       |   33.7 ns |      6.8 ns |             5.2 ns |        14.9 ns |
| `wait_already_set` t=1  |   16.5 ns |      3.2 ns |             3.2 ns |         6.8 ns |
| `wait_already_set` t=32 |    960 ns |      3.6 ns |             5.2 ns |         7.5 ns |
| `waiter_fan_out/1`      |    200 ns |      190 ns |             164 ns |         147 ns |
| `waiter_fan_out/8`      |    906 ns |     1.06 µs |             812 ns |         593 ns |
| `waiter_fan_out/32`     |   3.50 µs |     4.30 µs |            2.85 µs |        2.50 µs |
| `wake_waiter`           |   99.7 ns |     96.6 ns |            78.6 ns |        56.4 ns |
| `wake_waiter_reused`    |   81.8 ns |     99.7 ns |            69.6 ns |        61.4 ns |

- asyncband's `ManualResetEvent` is fully locked, including `is_set` and `set` without waiter.
- async-event and maillon `WaitList` share the no-waiter path (`SeqCst` fence + atomic load);
  async-event allocates a boxed notifier for each registered waiter.
- maillon `WaitList`: `AtomicBool` flag + `WaitList<(), Synchronized, AtomicLazy>`. `AtomicLazy` is
  5–18% faster than `AtomicEager` on every benchmark with a waiter.
- maillon `List`: `List<Waiter, usize, (), AtomicLazy>`, the set/unset state embedded in the list
  state. The condition check and the waiter push are a single CAS, but `set` and `reset` must CAS
  the tail word, hence the slower `set_reset_cycle`.

## futures-intrusive

`cargo bench --bench futures_intrusive -- tokio_rt/<implementation>/` (criterion).

futures-intrusive's own semaphore benchmark: 200 tasks on the tokio multi-thread runtime, each
acquiring 50 times and yielding 4 times while holding the permit.

| Contention          | maillon |   tokio | futures_intrusive (fair) | futures_intrusive (unfair) |
|---------------------|--------:|--------:|-------------------------:|---------------------------:|
| heavy (100 permits) | 4.34 ms | 9.14 ms |                  7.97 ms |                    9.94 ms |
| normal (180)        | 3.12 ms | 5.67 ms |                  6.78 ms |                    5.53 ms |
| none (200)          | 2.73 ms | 3.43 ms |                  4.15 ms |                    4.16 ms |

## async-channel

`cargo bench --bench async_channel -- '^<implementation>/' --quick` (criterion).

async-channel's `try_send`/`try_recv` on a bounded channel, reimplemented over the same
`ConcurrentQueue` with each notifier; 1000 messages, nobody ever waits, so it measures the notify
without listener: `try_send` notifies twice, `try_recv` once.

| Implementation                     | send (ns/msg) | recv (ns/msg) |
|------------------------------------|--------------:|--------------:|
| async-channel                      |          52.0 |          33.5 |
| event-listener copy                |          54.0 |          32.2 |
| async-event                        |          22.6 |          14.7 |
| maillon `WaitList`                 |          21.4 |          14.0 |
| maillon `WaitList<(), Sequential>` |          13.3 |          13.5 |

- event-listener 5.4 has no fast path without listener: every notify is a `SeqCst` fence plus a
  mutex lock/unlock.
- `Sequential` drops the fence; with the queue's `SeqCst` CAS and `full_fence()` on its empty/full
  paths it is sound, except for a sender waiting on a full `bounded(1)` channel, whose `Single`
  queue frees the slot with a `Release` RMW only.
- `recv` is bimodal under `--quick` (around 14 ns or 22 ns depending on the run for every fast
  implementation); the lower mode is reported.
