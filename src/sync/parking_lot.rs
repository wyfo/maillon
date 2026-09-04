use core::{mem::ManuallyDrop, ptr};

use parking_lot_core::{DEFAULT_PARK_TOKEN, DEFAULT_UNPARK_TOKEN};

use crate::sync::{condvar::CondVar, mutex::Mutex, parker::Parker};

/// A [`Parker`] built directly on `parking_lot_core`, which is what
/// [`parking_lot::Condvar`] is itself built on: going through a mutex and a condition
/// variable would only stack two layers on top of the very same parking lot.
///
/// The bucket lock `parking_lot_core` holds while running `validate` plays the role a
/// condition variable would give to its mutex, so the notification cannot be missed: either
/// [`unpark_one`](parking_lot_core::unpark_one) takes the bucket first, and `validate`
/// observes the notification and aborts the park, or `validate` takes it first and the
/// thread is enqueued before the unparker can look at the queue.
#[derive(Debug)]
pub struct ParkingLotParker(
    // `parking_lot_core` is keyed by address, so this byte is not dead weight: a zero-sized
    // parker could share its address with another field of the list — including its
    // `parking_lot::RawMutex`, which keys its own parking the same way — and the two would
    // then share a queue. A byte of storage guarantees a distinct key.
    #[allow(dead_code)] u8,
);

impl ParkingLotParker {
    #[inline]
    fn key(&self) -> usize {
        ptr::from_ref(self) as usize
    }
}

impl Parker for ParkingLotParker {
    const INIT: Self = Self(0);

    #[inline]
    unsafe fn park_until<T>(&self, mut notified: impl FnMut() -> Option<T>) -> T {
        loop {
            let mut res = None;
            // SAFETY: the key is the address of this parker, whose storage belongs to the
            // list, and `validate` neither panics nor calls into `parking_lot`.
            unsafe {
                parking_lot_core::park(
                    self.key(),
                    || {
                        res = notified();
                        res.is_none()
                    },
                    || {},
                    |_, _| {},
                    DEFAULT_PARK_TOKEN,
                    None,
                );
            }
            if let Some(res) = res {
                return res;
            }
        }
    }

    #[inline]
    fn unpark(&self) {
        // SAFETY: the key is the address of this parker, whose storage belongs to the list,
        // and the callback neither panics nor calls into `parking_lot`.
        unsafe { parking_lot_core::unpark_one(self.key(), |_| DEFAULT_UNPARK_TOKEN) };
    }
}

// SAFETY: `parking_lot::Condvar::wait` reacquires the mutex before returning, and
// `notify_one` synchronizes-with the woken `wait` calls through the mutex.
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
