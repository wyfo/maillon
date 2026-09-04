extern crate alloc;

use alloc::boxed::Box;
use core::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    ptr,
    ptr::NonNull,
    sync::atomic::{AtomicPtr, Ordering::*},
};

use crate::sync::{condvar::CondVar, mutex::Mutex};

fn unwrap(err_code: i32) {
    if err_code != 0 {
        #[cold]
        #[inline(never)]
        fn panic_libc(err_code: i32) -> ! {
            panic!("error {err_code}");
        }
        panic_libc(err_code);
    }
}

#[derive(Debug)]
struct LazyBox<T>(AtomicPtr<T>);

impl<T> LazyBox<T> {
    const fn new() -> Self {
        Self(AtomicPtr::new(ptr::null_mut()))
    }

    #[inline]
    fn get_or_init(&self, f: impl FnOnce() -> Box<T>) -> NonNull<T> {
        NonNull::new(self.0.load(Acquire)).unwrap_or_else(|| self.init(f))
    }

    #[cold]
    #[inline(never)]
    fn init(&self, f: impl FnOnce() -> Box<T>) -> NonNull<T> {
        let new_ptr = Box::into_raw(f());
        if let Err(ptr) = (self.0).compare_exchange(ptr::null_mut(), new_ptr, Release, Acquire) {
            drop(unsafe { Box::from_raw(new_ptr) });
            return NonNull::new(ptr).unwrap();
        }
        NonNull::new(new_ptr).unwrap()
    }
}

impl<T> Drop for LazyBox<T> {
    fn drop(&mut self) {
        if let Some(ptr) = NonNull::new(self.0.load(Acquire)) {
            drop(unsafe { Box::from_raw(ptr.as_ptr()) });
        }
    }
}

#[derive(Debug)]
struct RawMutex(UnsafeCell<libc::pthread_mutex_t>);

impl RawMutex {
    fn init() -> Box<Self> {
        struct AttrGuard<'a>(pub &'a mut MaybeUninit<libc::pthread_mutexattr_t>);
        impl Drop for AttrGuard<'_> {
            fn drop(&mut self) {
                unsafe { unwrap(libc::pthread_mutexattr_destroy(self.0.as_mut_ptr())) };
            }
        }
        let mutex = Box::new(Self(UnsafeCell::new(libc::PTHREAD_MUTEX_INITIALIZER)));
        unsafe {
            let mut attr = MaybeUninit::<libc::pthread_mutexattr_t>::uninit();
            unwrap(libc::pthread_mutexattr_init(attr.as_mut_ptr()));
            let attr = AttrGuard(&mut attr);
            unwrap(libc::pthread_mutexattr_settype(
                attr.0.as_mut_ptr(),
                libc::PTHREAD_MUTEX_NORMAL,
            ));
            unwrap(libc::pthread_mutex_init(mutex.0.get(), attr.0.as_ptr()));
            mutex
        }
    }
}

impl Drop for RawMutex {
    fn drop(&mut self) {
        unsafe { libc::pthread_mutex_destroy(self.0.get()) };
    }
}

#[derive(Debug)]
pub struct PthreadMutex(LazyBox<RawMutex>);

unsafe impl Send for PthreadMutex {}
unsafe impl Sync for PthreadMutex {}

impl PthreadMutex {
    fn raw(&self) -> *mut libc::pthread_mutex_t {
        unsafe { UnsafeCell::raw_get(&raw const (*self.0.get_or_init(RawMutex::init).as_ptr()).0) }
    }
}

unsafe impl Mutex for PthreadMutex {
    const INIT: Self = Self(LazyBox::new());
    type Guard<'a>
        = ()
    where
        Self: 'a;

    fn lock(&self) -> Self::Guard<'_> {
        unwrap(unsafe { libc::pthread_mutex_lock(self.raw()) });
    }

    unsafe fn unlock<'a>(&'a self, _guard: Self::Guard<'a>) {
        unwrap(unsafe { libc::pthread_mutex_unlock(self.raw()) });
    }
}

#[derive(Debug)]
struct RawCondvar(UnsafeCell<libc::pthread_cond_t>);

impl RawCondvar {
    fn init() -> Box<Self> {
        Box::new(Self(UnsafeCell::new(libc::PTHREAD_COND_INITIALIZER)))
    }
}

impl Drop for RawCondvar {
    #[inline]
    fn drop(&mut self) {
        unsafe { libc::pthread_cond_destroy(self.0.get()) };
    }
}

#[derive(Debug)]
pub struct PthreadCondVar(LazyBox<RawCondvar>);

unsafe impl Send for PthreadCondVar {}
unsafe impl Sync for PthreadCondVar {}

impl PthreadCondVar {
    fn raw(&self) -> *mut libc::pthread_cond_t {
        unsafe {
            UnsafeCell::raw_get(&raw const (*self.0.get_or_init(RawCondvar::init).as_ptr()).0)
        }
    }
}

// SAFETY: `pthread_cond_wait` reacquires the mutex before returning, and
// `pthread_cond_broadcast` synchronizes-with the woken `pthread_cond_wait` calls through the
// mutex.
unsafe impl CondVar<PthreadMutex> for PthreadCondVar {
    const INIT: Self = Self(LazyBox::new());

    // the guard is returned as required by the trait, even though it is `()` here
    #[allow(clippy::semicolon_if_nothing_returned)]
    #[inline]
    unsafe fn wait<'a>(
        &self,
        mutex: &'a PthreadMutex,
        guard: <PthreadMutex as Mutex>::Guard<'a>,
    ) -> <PthreadMutex as Mutex>::Guard<'a> {
        unwrap(unsafe { libc::pthread_cond_wait(self.raw(), mutex.raw()) });
        guard
    }

    #[inline]
    fn notify_one(&self) {
        unwrap(unsafe { libc::pthread_cond_signal(self.raw()) });
    }
}

pub type PthreadParker = super::parker::CondVarParker<PthreadMutex, PthreadCondVar>;
