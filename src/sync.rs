//! The synchronization primitive abstractions ([`Mutex`](mutex::Mutex) and
//! [`Parker`](parker::Parker)) used by [`List`](crate::List), and their implementations for the
//! supported backends.
#![warn(missing_docs)]
#[cfg(feature = "atomic-wait")]
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
