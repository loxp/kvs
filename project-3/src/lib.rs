#![deny(missing_docs)]
//! A simple key/value store.

pub use engine::{build_engine, EngineType, KvStore, KvsEngine};
pub use error::KvsError;
pub use model::Result;
pub use server::KvsServer;

mod engine;
mod error;
mod model;
mod server;
