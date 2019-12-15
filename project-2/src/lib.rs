#![deny(missing_docs)]
//! A simple key/value store.

pub use error::KvsError;
pub use kvs::{KvStore, Result};

/// error
pub mod error;
/// kvs
pub mod kvs;
