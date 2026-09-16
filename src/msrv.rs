// TODO 1.70: Option::is_some_and
// TODO 1.76: Result::inspect, ptr::from_ref, ptr::from_mut
// TODO 1.82: Option::is_none_or
// TODO 1.84: strict provenance
// TODO 1.91: AtomicPtr::fetch_byte_add (loom.rs)
#![allow(clippy::incompatible_msrv, unstable_name_collisions)]

use core::{num::NonZeroUsize, ptr::NonNull};

#[allow(dead_code)]
pub(crate) trait OptionExt<T> {
    #[allow(clippy::wrong_self_convention)]
    fn is_some_and(self, f: impl FnOnce(T) -> bool) -> bool;
    #[allow(clippy::wrong_self_convention)]
    fn is_none_or(self, f: impl FnOnce(T) -> bool) -> bool;
}

impl<T> OptionExt<T> for Option<T> {
    fn is_some_and(self, f: impl FnOnce(T) -> bool) -> bool {
        match self {
            None => false,
            Some(x) => f(x),
        }
    }

    fn is_none_or(self, f: impl FnOnce(T) -> bool) -> bool {
        match self {
            None => true,
            Some(x) => f(x),
        }
    }
}

#[allow(dead_code)]
pub(crate) trait ResultExt<T, E> {
    fn inspect(self, f: impl FnOnce(&T)) -> Self;
}

impl<T, E> ResultExt<T, E> for Result<T, E> {
    fn inspect(self, f: impl FnOnce(&T)) -> Self {
        if let Ok(t) = &self {
            f(t);
        }
        self
    }
}

#[allow(dead_code)]
pub(crate) trait StrictProvenance<T>: Sized + Copy {
    type Addr;
    fn addr(self) -> Self::Addr;
    fn with_addr(self, addr: Self::Addr) -> Self;
    fn map_addr(self, f: impl FnOnce(Self::Addr) -> Self::Addr) -> Self {
        self.with_addr(f(self.addr()))
    }
}

impl<T> StrictProvenance<T> for *mut T {
    type Addr = usize;
    fn addr(self) -> usize {
        self as usize
    }
    fn with_addr(self, addr: usize) -> Self {
        let ptr_addr = self as isize;
        let dest_addr = addr as isize;
        let offset = dest_addr.wrapping_sub(ptr_addr);
        self.cast::<u8>().wrapping_offset(offset).cast()
    }
}

impl<T> StrictProvenance<T> for *const T {
    type Addr = usize;
    fn addr(self) -> usize {
        self as usize
    }
    fn with_addr(self, addr: usize) -> Self {
        self.cast_mut().with_addr(addr).cast_const()
    }
}

impl<T> StrictProvenance<T> for NonNull<T> {
    type Addr = NonZeroUsize;
    fn addr(self) -> NonZeroUsize {
        unsafe { NonZeroUsize::new_unchecked(self.as_ptr().addr()) }
    }
    fn with_addr(self, addr: NonZeroUsize) -> Self {
        unsafe { NonNull::new_unchecked(self.as_ptr().with_addr(addr.get())) }
    }
}

pub(crate) mod ptr {
    pub(crate) use core::ptr::*;

    pub(crate) fn from_ref<T: ?Sized>(t: &T) -> *const T {
        t as _
    }

    pub(crate) fn from_mut<T: ?Sized>(t: &mut T) -> *mut T {
        t as _
    }

    pub(crate) const fn without_provenance_mut<T>(addr: usize) -> *mut T {
        null_mut::<u8>().wrapping_add(addr).cast()
    }
}
