use core::{array, mem, mem::MaybeUninit, task::Waker};

pub(super) struct WakerList<const N: usize> {
    wakers: [MaybeUninit<Waker>; N],
    len: usize,
}

impl<const N: usize> WakerList<N> {
    pub(super) fn new() -> Self {
        const { assert!(N > 0, "WAKER_LIST_SIZE must be greater than 0") };
        Self {
            wakers: array::from_fn(|_| MaybeUninit::uninit()),
            len: 0,
        }
    }

    pub(super) fn push(&mut self, waker: Waker) {
        self.wakers[self.len].write(waker);
        self.len += 1;
    }

    pub(super) fn is_full(&self) -> bool {
        self.len == self.wakers.len()
    }

    fn drain_with(&mut self, f: impl Fn(Waker)) {
        let len = self.len;
        self.len = 0;
        struct Drain<'a>(&'a mut [MaybeUninit<Waker>]);
        impl Drop for Drain<'_> {
            fn drop(&mut self) {
                for waker in self.0.iter_mut() {
                    unsafe { waker.assume_init_drop() };
                }
            }
        }
        let mut drain = Drain(unsafe { self.wakers.get_unchecked_mut(..len) });
        while let [waker, remain @ ..] = mem::take(&mut drain.0) {
            drain.0 = remain;
            f(unsafe { waker.assume_init_read() });
        }
    }

    pub(super) fn wake_all(&mut self) {
        self.drain_with(Waker::wake);
    }
}

impl<const N: usize> Drop for WakerList<N> {
    fn drop(&mut self) {
        self.drain_with(drop);
    }
}
