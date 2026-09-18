//! Shared parametrization marker for the `Notify`/`Semaphore` test suites.
//!
//! `rstest` cannot vary a type parameter directly, so each variant is represented by a
//! zero-sized value whose type drives `L`. A parametrized test takes one of these as an
//! otherwise unused argument and turbofishes `L` at the constructor.

use std::marker::PhantomData;

use aiq::linking::{AtomicEager, AtomicLazy, Linking, Serialized};

pub struct LinkingMode<L: Linking>(PhantomData<L>);

pub const EAGER: LinkingMode<AtomicEager> = LinkingMode(PhantomData);
pub const LAZY: LinkingMode<AtomicLazy> = LinkingMode(PhantomData);
pub const SERIALIZED: LinkingMode<Serialized> = LinkingMode(PhantomData);
