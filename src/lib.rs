//! Bitswap protocol implementation
#![deny(missing_docs)]
#![deny(warnings)]
#![allow(clippy::derive_partial_eq_without_eq)]

mod behaviour;
#[cfg(feature = "compat")]
mod compat;
mod protocol;
mod query;
mod stats;
mod token;

pub use crate::behaviour::{Bitswap, BitswapConfig, BitswapEvent, BitswapStore, Channel};
pub use crate::query::QueryId;
pub use token::Token;
