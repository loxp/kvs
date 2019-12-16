#![deny(missing_docs)]
//! A simple key/value store.

pub use crate::kvs::{KvStore, Result};
pub use error::KvsError;

/// error
pub mod error;
/// kvs
pub mod kvs;
