//! A concurrent intrusive list with lock-free insertion, mainly for building synchronization
//! primitives.
//!
//! *Maillon is the French word for a chain link.*
//!
//! # Features
//!
//! - 100% safe API
//! - `#![no_std]`, no allocation
//! - Atomic emptiness check to avoid acquiring the mutex if the list is empty
//! - Lock-free[^1] insertion: multiple nodes can be inserted concurrently while another is being
//!   removed; removal requires locking
//! - Optional atomic state embedded in the list when empty (to carry a semaphore counter, a closed
//!   flag, etc.)
//! - [`WaitList`], a high-level asynchronous wait list with customizable synchronization built on
//!   top of the low-level [`List`]
//!
//! # Usage
//!
//! [`WaitList`] is a ready-to-use asynchronous wait list, built on top of [`List`]:
//!
//! ```
//! use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
//!
//! use maillon::WaitList;
//!
//! #[derive(Default)]
//! pub struct Event {
//!     done: AtomicBool,
//!     wait_list: WaitList,
//! }
//!
//! impl Event {
//!     pub async fn wait(&self) {
//!         let _ = self.wait_list.wait_until(|_| self.done.load(Relaxed)).await;
//!     }
//!
//!     pub fn set(&self) {
//!         self.done.store(true, Relaxed);
//!         self.wait_list.notify_all();
//!     }
//! }
//! ```
//!
//! [`List`] is the building block: [`Node`]s are pinned, carry user data implementing
//! [`NodeData`], and are pushed to the back without locking, while every other operation goes
//! through [`List::lock`]. Here is a minimal wait list, supporting only `notify_one`:
//!
//! ```
//! use std::{
//!     future::Future,
//!     mem,
//!     pin::Pin,
//!     sync::atomic::{
//!         Ordering::{Relaxed, SeqCst},
//!         fence,
//!     },
//!     task::{Context, Poll, Waker},
//! };
//!
//! use maillon::{List, Node, NodeData, NodeState, list::LockedList, node_wrapper};
//!
//! #[derive(Default)]
//! pub struct WaitList {
//!     list: List<Waiter>,
//! }
//!
//! #[derive(Default)]
//! struct Waiter {
//!     waker: Option<Waker>,
//!     notified: bool,
//! }
//!
//! impl WaitList {
//!     pub fn notify_one(&self) {
//!         fence(SeqCst);
//!         if !self.list.is_empty(Relaxed) {
//!             Self::notify_one_cold(&self.list);
//!         }
//!     }
//!
//!     #[cold]
//!     fn notify_one_cold(list: &List<Waiter>) {
//!         Self::notify_one_locked(list.lock());
//!     }
//!
//!     fn notify_one_locked(mut locked: LockedList<'_, Waiter>) {
//!         let Some(mut front) = locked.front() else {
//!             return;
//!         };
//!         front.notified = true;
//!         let waker = front.waker.take();
//!         front.unlink();
//!         drop(locked);
//!         if let Some(waker) = waker {
//!             waker.wake();
//!         }
//!     }
//!
//!     pub fn wait(&self) -> Wait<'_> {
//!         Wait(Node::new(&self.list))
//!     }
//! }
//!
//! node_wrapper! {
//!     pub struct Wait<'a>(Node<&'a List<Waiter>>);
//! }
//!
//! impl Future for Wait<'_> {
//!     type Output = ();
//!
//!     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
//!         match self.node_mut().state() {
//!             NodeState::Unlinked(mut node) => {
//!                 if mem::take(&mut node.notified) {
//!                     return Poll::Ready(());
//!                 }
//!                 node.waker = Some(cx.waker().clone());
//!                 node.push_back(Relaxed);
//!                 fence(SeqCst);
//!             }
//!             NodeState::Linked(mut node) => {
//!                 if node.waker.as_ref().is_none_or(|w| !w.will_wake(cx.waker())) {
//!                     node.waker = Some(cx.waker().clone());
//!                 }
//!             }
//!         }
//!         Poll::Pending
//!     }
//! }
//!
//! impl NodeData<&List<Waiter>> for Waiter {
//!     fn new_state_if_last_node_on_drop(
//!         self: Pin<&mut Self>,
//!         _list: &&List<Waiter>,
//!         _list_data: &mut (),
//!     ) {
//!     }
//!
//!     fn on_drop<'list>(
//!         self: Pin<&mut Self>,
//!         list: &'list &List<Waiter>,
//!         locked: Option<LockedList<'list, Self>>,
//!         _state_updated_on_unlink: bool,
//!     ) {
//!         if self.notified {
//!             match locked {
//!                 Some(locked) => WaitList::notify_one_locked(locked),
//!                 None => WaitList::notify_one_cold(list),
//!             }
//!         }
//!     }
//! }
//! ```
//!
//! See the [examples] for full implementations of `tokio::sync::Notify` and
//! `tokio::sync::Semaphore` built with `maillon`, with fully identical API and behavior.
//!
//! [examples]: https://github.com/wyfo/maillon/tree/main/examples
//! [^1]: In some rare cases, an inserting thread might need to unpark a remover thread, making
//!     insertion not strictly lock-free. It is also possible to switch the list to lazy node
//!     linking, making the node insertion fully lock-free. A third option is serialized linking,
//!     where insertion requires locking but can then happen at any position through a cursor, not
//!     only at the back.
#![cfg_attr(docsrs, feature(doc_cfg))]
#![no_std]
#![cfg_attr(nightly, feature(unsafe_pinned))]

#[cfg(feature = "std")]
extern crate std;

pub mod linking;
pub mod list;
mod loom;
mod macros;
mod msrv;
pub mod node;
pub mod sync;
#[cfg(not(nightly))]
mod unsafe_pinned;
mod utils;
pub mod wait_list;
mod waker_batch;

pub use atomic_backoff as backoff;
pub use list::{List, ListRef, LockedList};
pub use node::{Node, NodeData, NodeState};
pub use wait_list::WaitList;
pub use waker_batch::WakerBatch;
