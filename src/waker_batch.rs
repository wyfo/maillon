use core::{array, fmt, mem, mem::MaybeUninit, task::Waker};

/// A fixed-capacity buffer of [`Waker`]s, stored inline.
///
/// It allows collecting wakers while a lock is held, and waking them once the lock is released,
/// without any allocation. When the batch is full, [`wake_all`](Self::wake_all) should be called
/// outside the critical section before it can be filled again.
pub struct WakerBatch<const SIZE: usize> {
    wakers: [MaybeUninit<Waker>; SIZE],
    len: usize,
}

impl<const SIZE: usize> WakerBatch<SIZE> {
    const SIZE_CHECK: () = assert!(SIZE > 0, "WakerBatch size must be greater than 0");

    /// Creates an empty batch.
    pub fn new() -> Self {
        let () = Self::SIZE_CHECK;
        Self {
            wakers: array::from_fn(|_| MaybeUninit::uninit()),
            len: 0,
        }
    }

    /// Appends a waker to the batch.
    ///
    /// # Panics
    ///
    /// Panics if the batch [is full](Self::is_full).
    pub fn push(&mut self, waker: Waker) {
        self.wakers
            .get_mut(self.len)
            .expect("WakerBatch is full")
            .write(waker);
        self.len += 1;
    }

    /// Returns `true` if the batch contains `SIZE` wakers.
    pub fn is_full(&self) -> bool {
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

    /// Wakes all the wakers of the batch, leaving it empty.
    pub fn wake_all(&mut self) {
        self.drain_with(Waker::wake);
    }
}

impl<const SIZE: usize> Drop for WakerBatch<SIZE> {
    fn drop(&mut self) {
        self.drain_with(drop);
    }
}

impl<const SIZE: usize> Default for WakerBatch<SIZE> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const SIZE: usize> fmt::Debug for WakerBatch<SIZE> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (_, wakers, _) = unsafe { self.wakers[..self.len].align_to::<Waker>() };
        f.debug_tuple("WakerBatch").field(&wakers).finish()
    }
}
