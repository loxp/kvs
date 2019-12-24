use crate::error::KvsError;
use crate::model::Result;
use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

mod kvs;
mod sled;

pub use self::kvs::KvStore;
pub use self::sled::SledKvsEngine;

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

impl EngineType {
    /// Check whether the engine type is valid
    pub fn check(&self) -> Result<()> {
        let mut engine_type_file_path = self.get_path()?;
        engine_type_file_path.push("kvs");
        engine_type_file_path.set_extension("engine");
        if !engine_type_file_path.exists() {
            let mut file = File::create(engine_type_file_path)?;
            let engine_name = self.get_name();
            file.write_all(engine_name.as_bytes())?;
            file.sync_all()?;
            Ok(())
        } else {
            let origin_engine_name = Self::get_engine_type_from_file(engine_type_file_path)?;
            let current_engine_name = self.get_name();
            if origin_engine_name == current_engine_name {
                Ok(())
            } else {
                Err(KvsError::InvalidStorageEngineType)
            }
        }
    }

    fn get_name(&self) -> String {
        match *self {
            EngineType::Kvs(_) => "kvs".to_string(),
            EngineType::Sled(_) => "sled".to_string(),
        }
    }

    fn get_path(&self) -> Result<PathBuf> {
        match self {
            EngineType::Kvs(path) => Ok(path.clone()),
            EngineType::Sled(path) => Ok(path.clone()),
        }
    }

    fn get_engine_type_from_file(engine_type_file: PathBuf) -> Result<String> {
        let mut file = File::open(engine_type_file)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        Ok(contents)
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
