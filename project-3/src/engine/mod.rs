use crate::error::KvsError::InternalError;
use crate::model::Result;
use std::path::{Path, PathBuf};

mod kvs;

pub use kvs::KvStore;

/// Store engine abstraction of kvs
pub trait KvsEngine: Send + Sync {
    /// Set a key value pair.
    /// If set success, then return Ok(()),
    /// Return Err(e) when error occurs.
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// Get value by key
    /// If get success, return a Option.
    /// Return Err(e) when error occurs.
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// Remove value by key
    /// If remove success, return Ok(()).
    /// Return Err(e) when error occurs.
    fn remove(&mut self, key: String) -> Result<()>;
}

/// Enum type of engine
pub enum EngineType {
    /// Kvs engine
    Kvs(PathBuf),
    /// Sled engine
    Sled(PathBuf),
}

/// Build engine by engine type
pub fn build_engine(engine_type: EngineType) -> Result<impl KvsEngine> {
    match engine_type {
        EngineType::Kvs(path) => KvStore::open(path),
        _ => Err(InternalError),
    }
}

#[cfg(test)]
mod tests {
    use super::{build_engine, EngineType, EngineType::Kvs};
    use std::path::PathBuf;
    use tempfile::TempDir;

    #[test]
    fn test_build_engine() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let engine_type = EngineType::Kvs(temp_dir.into_path());
        let engine = build_engine(engine_type);
    }
}
