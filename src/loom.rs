cfg_if::cfg_if! {
    if #[cfg(loom)] {
        pub(crate) use loom::{cell, sync};
    } else if #[cfg(feature = "std")] {
        extern crate std;
        pub(crate) use std::{cell, sync};
    } else {
        pub(crate) use core::{cell, sync};
    }
}

pub(crate) trait AtomicPtrExt<T> {
    fn load_mut(&mut self) -> *mut T;
    fn store_mut(&mut self, ptr: *mut T);
    #[cfg(loom)]
    fn fetch_byte_add(&self, val: usize, order: sync::atomic::Ordering) -> *mut T;
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

    #[cfg(loom)]
    fn fetch_byte_add(&self, val: usize, order: sync::atomic::Ordering) -> *mut T {
        unsafe { &*(self as *const _ as *const sync::atomic::AtomicUsize) }.fetch_add(val, order)
            as _
    }
}
