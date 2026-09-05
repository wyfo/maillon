use crate::{
    loom::sync::{Condvar, Mutex, MutexGuard},
    sync::condvar::CondVar,
};

#[cold]
#[inline(never)]
fn panic_lock() -> ! {
    panic!("poisoned lock: another task failed inside");
}

#[derive(Debug)]
pub struct StdMutex(Mutex<()>);

unsafe impl super::mutex::Mutex for StdMutex {
    #[cfg(not(loom))]
    #[allow(clippy::declare_interior_mutable_const)]
    const INIT: Self = Self(Mutex::new(()));
    #[cfg(loom)]
    const INIT: Self = unimplemented!();
    fn new() -> Self {
        Self(Mutex::new(()))
    }
    type Guard<'a>
        = MutexGuard<'a, ()>
    where
        Self: 'a;
    #[inline]
    fn lock(&self) -> Self::Guard<'_> {
        self.0.lock().unwrap_or_else(|_| panic_lock())
    }
    #[inline]
    unsafe fn unlock<'a>(&'a self, guard: Self::Guard<'a>) {
        drop(guard);
    }
}

pub type StdParker = super::parker::CondVarParker<StdMutex, StdCondVar>;

#[derive(Debug)]
pub struct StdCondVar(Condvar);

// SAFETY: `Condvar::wait` reacquires the mutex before returning, and `notify_all`
// synchronizes-with the woken `wait` calls through the mutex.
unsafe impl CondVar<StdMutex> for StdCondVar {
    #[cfg(not(loom))]
    #[allow(clippy::declare_interior_mutable_const)]
    const INIT: Self = Self(Condvar::new());
    #[cfg(loom)]
    const INIT: Self = unimplemented!();
    fn new() -> Self {
        Self(Condvar::new())
    }

    #[inline]
    unsafe fn wait<'a>(
        &self,
        _mutex: &'a StdMutex,
        guard: <StdMutex as super::mutex::Mutex>::Guard<'a>,
    ) -> <StdMutex as super::mutex::Mutex>::Guard<'a> {
        self.0.wait(guard).unwrap_or_else(|_| panic_lock())
    }

    #[inline]
    fn notify_one(&self) {
        self.0.notify_one();
    }
}
