//! The synchronization primitive abstractions ([`Mutex`](mutex::Mutex) and
//! [`Parker`](parker::Parker)) used by [`List`](crate::List), and their implementations for the
//! supported backends.
#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "freebsd",
    target_os = "macos",
    target_os = "ios",
    target_os = "watchos",
    windows
))]
mod atomic_wait;
pub mod condvar;
pub mod mutex;
pub mod parker;
#[cfg(feature = "parking_lot")]
mod parking_lot;
#[cfg(all(feature = "pthread", unix))]
mod pthread;
mod spin;
#[cfg(feature = "std")]
mod std;
