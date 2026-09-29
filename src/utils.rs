use core::{
    marker::PhantomData, mem, mem::ManuallyDrop, ptr, ptr::NonNull, sync::atomic::Ordering,
};

#[allow(unused_imports)]
use crate::msrv::StrictProvenance;
use crate::{
    loom::{
        AtomicPtrExt,
        sync::atomic::{AtomicPtr, AtomicUsize},
    },
    msrv,
};

pub trait OptionNonNullExt<T> {
    #[allow(clippy::wrong_self_convention)]
    fn as_ptr(self) -> *mut T;
}

impl<T> OptionNonNullExt<T> for Option<NonNull<T>> {
    fn as_ptr(self) -> *mut T {
        self.map_or(ptr::null_mut(), NonNull::as_ptr)
    }
}

pub fn defer(f: impl FnOnce()) -> impl Drop {
    struct Defer<F: FnOnce()>(ManuallyDrop<F>);
    impl<F: FnOnce()> Drop for Defer<F> {
        fn drop(&mut self) {
            unsafe { ManuallyDrop::take(&mut self.0)() };
        }
    }
    Defer(ManuallyDrop::new(f))
}

#[inline]
pub fn abort_on_unwind<R>(f: impl FnOnce() -> R) -> R {
    #[cold]
    #[inline(never)]
    fn panic_on_unwind() -> ! {
        panic!("unwinding is not allowed here");
    }
    let bomb = defer(|| panic_on_unwind());
    let res = f();
    mem::forget(bomb);
    res
}

pub trait AtomicPtrImpl<T>: AtomicPtrExt<T> + Send + Sync {
    fn new(ptr: *mut T) -> Self;
    fn load(&self, order: Ordering) -> *mut T;
    fn store(&self, ptr: *mut T, order: Ordering);
}

impl<T> AtomicPtrImpl<T> for AtomicPtr<T> {
    fn new(ptr: *mut T) -> Self {
        AtomicPtr::new(ptr)
    }
    fn load(&self, order: Ordering) -> *mut T {
        AtomicPtr::load(self, order)
    }
    fn store(&self, ptr: *mut T, order: Ordering) {
        AtomicPtr::store(self, ptr, order);
    }
}

pub struct ExposedAtomicPtr<T>(AtomicUsize, PhantomData<fn() -> *mut T>);

impl<T> ExposedAtomicPtr<T> {
    #[allow(clippy::declare_interior_mutable_const)]
    #[cfg(not(loom))]
    pub const NULL: Self = Self(AtomicUsize::new(0), PhantomData);
}

impl<T> AtomicPtrImpl<T> for ExposedAtomicPtr<T> {
    #[allow(clippy::incompatible_msrv, unstable_name_collisions)]
    fn new(ptr: *mut T) -> Self {
        Self(AtomicUsize::new(ptr.addr()), PhantomData)
    }
    fn load(&self, order: Ordering) -> *mut T {
        msrv::ptr::with_exposed_provenance_mut(self.0.load(order))
    }
    #[allow(clippy::incompatible_msrv, unstable_name_collisions)]
    fn store(&self, ptr: *mut T, order: Ordering) {
        self.0.store(ptr.addr(), order);
    }
}

impl<T> AtomicPtrExt<T> for ExposedAtomicPtr<T> {
    fn load_mut(&mut self) -> *mut T {
        #[cfg(not(loom))]
        let addr = *self.0.get_mut();
        #[cfg(loom)]
        let addr = self.0.with_mut(|addr| *addr);
        msrv::ptr::with_exposed_provenance_mut(addr)
    }
    #[allow(clippy::incompatible_msrv, unstable_name_collisions)]
    fn store_mut(&mut self, ptr: *mut T) {
        #[cfg(not(loom))]
        let () = *self.0.get_mut() = ptr.addr();
        #[cfg(loom)]
        self.0.with_mut(|addr| *addr = ptr.addr());
    }
}
