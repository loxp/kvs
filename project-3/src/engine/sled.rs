use crate::{KvsEngine, Result};
use std::path::{Path, PathBuf};

/// Sled kvs engine
pub struct SledKvsEngine {}

impl SledKvsEngine {
    /// Open and create SledKvsEngine
    pub fn open(path: PathBuf) -> Result<SledKvsEngine> {
        let engine = SledKvsEngine {};
        Ok(engine)
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(None)
    }

    fn remove(&mut self, key: String) -> Result<()> {
        Ok(())
    }
}
