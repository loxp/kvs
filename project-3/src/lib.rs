#![deny(missing_docs)]
//! A simple key/value store.

pub use client::KvsClient;
pub use engine::{EngineType, KvStore, KvsEngine, SledKvsEngine};
pub use error::KvsError;
pub use model::Result;
pub use server::KvsServer;

mod client;
/// encoding / decoding implementation of kvs
pub mod codec;
/// engines of kvs
pub mod engine;
mod error;
mod model;
mod server;
