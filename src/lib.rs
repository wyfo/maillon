#![cfg_attr(docsrs, feature(doc_cfg))]
#![no_std]
#![cfg_attr(nightly, feature(unsafe_pinned))]

pub mod list;
mod loom;
pub mod node;
pub mod sync;
#[cfg(not(nightly))]
mod unsafe_pinned;
mod utils;
// Temporarily disabled while `Queue`'s API is refactored; restored in step 8.
// #[cfg(feature = "wait-list")]
// pub mod wait_list;

pub use list::List;
pub use node::{Node, NodeState};
// #[cfg(feature = "wait-queue")]
// pub use wait_list::WaitList;
