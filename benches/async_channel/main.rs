macro_rules! channel_benches {
    ($name:literal, $bounded:expr) => {
        use std::hint::black_box;

        use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};

        const COUNTS: &[usize] = &[1, 100, 1000];

        pub fn benches(c: &mut Criterion) {
            let mut g = c.benchmark_group(concat!($name, "/send"));
            for &n in COUNTS {
                g.throughput(Throughput::Elements(n as u64));
                g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
                    b.iter_batched(
                        || $bounded(n),
                        |(s, r)| {
                            for i in 0..n {
                                s.try_send(black_box(i)).unwrap();
                            }
                            (s, r)
                        },
                        BatchSize::SmallInput,
                    );
                });
            }
            g.finish();

            let mut g = c.benchmark_group(concat!($name, "/recv"));
            for &n in COUNTS {
                g.throughput(Throughput::Elements(n as u64));
                g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
                    b.iter_batched(
                        || {
                            let (s, r) = $bounded(n);
                            for i in 0..n {
                                s.try_send(i).unwrap();
                            }
                            (s, r)
                        },
                        |(s, r)| {
                            for _ in 0..n {
                                black_box(r.try_recv().unwrap());
                            }
                            (s, r)
                        },
                        BatchSize::SmallInput,
                    );
                });
            }
            g.finish();
        }
    };
}

mod channel;

mod async_channel {
    channel_benches!("async_channel", ::async_channel::bounded::<usize>);
}

mod async_event {
    use crate::channel;

    channel_benches!(
        "async_event",
        channel::bounded::<usize, ::async_event::Event>
    );
}

mod event_listener {
    use crate::channel;

    channel_benches!(
        "event_listener",
        channel::bounded::<usize, ::event_listener::Event>
    );
}

mod maillon {
    use crate::channel;

    channel_benches!("maillon", channel::bounded::<usize, ::maillon::WaitList>);
}

mod maillon_sequential {
    use ::maillon::{WaitList, wait_list::synchronization::Sequential};

    use crate::channel;

    channel_benches!(
        "maillon_sequential",
        channel::bounded::<usize, WaitList<(), Sequential>>
    );
}

criterion::criterion_group!(
    benches,
    async_channel::benches,
    async_event::benches,
    event_listener::benches,
    maillon::benches,
    maillon_sequential::benches
);
criterion::criterion_main!(benches);
