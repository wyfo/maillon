#![cfg_attr(docsrs, feature(doc_cfg))]
#![no_std]
#![cfg_attr(nightly, feature(unsafe_pinned))]

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
