use crate::{
    loom::sync::{Condvar, Mutex, MutexGuard},
    sync::condvar::CondVar,
};

unsafe impl super::mutex::Mutex for Mutex<()> {
    #[cfg(not(loom))]
    #[allow(clippy::declare_interior_mutable_const)]
    const INIT: Self = Mutex::new(());
    #[cfg(loom)]
    const INIT: Self = unimplemented!();
    fn new() -> Self {
        Mutex::new(())
    }
    type Guard<'a>
        = MutexGuard<'a, ()>
    where
        Self: 'a;
    #[inline]
    fn lock(&self) -> Self::Guard<'_> {
        self.lock().unwrap_or_else(|err| err.into_inner())
    }
    #[inline]
    unsafe fn unlock<'a>(&'a self, guard: Self::Guard<'a>) {
        drop(guard);
    }
}

pub type StdParker = super::parker::CondVarParker<Mutex<()>, Condvar, false>;

// SAFETY: `Condvar::wait` reacquires the mutex before returning, and `notify_all`
// synchronizes-with the woken `wait` calls through the mutex.
unsafe impl CondVar<Mutex<()>> for Condvar {
    #[cfg(not(loom))]
    #[allow(clippy::declare_interior_mutable_const)]
    const INIT: Self = Condvar::new();
    #[cfg(loom)]
    const INIT: Self = unimplemented!();
    fn new() -> Self {
        Condvar::new()
    }

    #[inline]
    unsafe fn wait<'a>(
        &self,
        _mutex: &'a Mutex<()>,
        guard: <Mutex<()> as super::mutex::Mutex>::Guard<'a>,
    ) -> <Mutex<()> as super::mutex::Mutex>::Guard<'a> {
        self.wait(guard).unwrap_or_else(|err| err.into_inner())
    }

    #[inline]
    fn notify_one(&self) {
        self.notify_one();
    }
}
