use core::mem::ManuallyDrop;

use crate::sync::{condvar::CondVar, mutex::Mutex, parker::CondVarParker};

pub type ParkingLotParker = CondVarParker<parking_lot::Mutex<()>, parking_lot::Condvar>;

// SAFETY: `parking_lot::Condvar::wait` reacquires the mutex before returning, and
// `notify_all` synchronizes-with the woken `wait` calls through the mutex.
unsafe impl CondVar<parking_lot::Mutex<()>> for parking_lot::Condvar {
    const INIT: Self = parking_lot::Condvar::new();

    #[inline]
    unsafe fn wait<'a>(
        &self,
        mutex: &'a parking_lot::Mutex<()>,
        _guard: <parking_lot::Mutex<()> as Mutex>::Guard<'a>,
    ) -> <parking_lot::Mutex<()> as Mutex>::Guard<'a> {
        // SAFETY: this thread logically holds the lock, and the guard `Mutex::lock` acquired
        // has been forgotten.
        let mut guard = ManuallyDrop::new(unsafe { mutex.make_guard_unchecked() });
        parking_lot::Condvar::wait(self, &mut guard);
    }

    #[inline]
    fn notify_one(&self) {
        parking_lot::Condvar::notify_one(self);
    }
}
