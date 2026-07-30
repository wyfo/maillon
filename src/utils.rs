use core::{mem::ManuallyDrop, ptr, ptr::NonNull};

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
