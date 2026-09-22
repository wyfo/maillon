#[cfg(not(loom))]
pub(crate) use core::cell;

#[cfg(loom)]
pub(crate) use loom::{cell, sync};
#[cfg(not(loom))]
pub(crate) mod sync {
    #[cfg(not(feature = "portable-atomic"))]
    pub(crate) use core::sync::atomic;
    #[cfg(feature = "std")]
    pub(crate) use std::sync::{Condvar, Mutex, MutexGuard};

    #[cfg(feature = "portable-atomic")]
    pub(crate) use portable_atomic as atomic;
}

pub(crate) trait AtomicPtrExt<T> {
    fn load_mut(&mut self) -> *mut T;
    fn store_mut(&mut self, ptr: *mut T);
}

impl<T> AtomicPtrExt<T> for sync::atomic::AtomicPtr<T> {
    fn load_mut(&mut self) -> *mut T {
        #[cfg(not(loom))]
        return *self.get_mut();
        #[cfg(loom)]
        return self.with_mut(|ptr| *ptr);
    }

    fn store_mut(&mut self, ptr: *mut T) {
        #[cfg(not(loom))]
        let () = *self.get_mut() = ptr;
        #[cfg(loom)]
        return self.with_mut(|p| *p = ptr);
    }
}
