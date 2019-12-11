#![deny(missing_docs)]
//! A simple key/value store.

pub use error::KvsError;
pub use kvs::{KvStore, Result};

mod error;
mod kvs;
