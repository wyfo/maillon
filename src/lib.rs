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
#[cfg(feature = "wait-list")]
pub mod wait_list;

pub use list::{List, ListRef};
pub use node::{Node, NodeData, NodeState};
#[cfg(feature = "wait-list")]
pub use wait_list::WaitList;
