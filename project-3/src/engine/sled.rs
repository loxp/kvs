use crate::{KvsEngine, KvsError, Result};
use sled::{Db, IVec};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Sled kvs engine
pub struct SledKvsEngine {
    db: Db,
}

impl SledKvsEngine {
    /// Open and create SledKvsEngine
    pub fn open(path: PathBuf) -> Result<SledKvsEngine> {
        let db = Db::open(path).map_err(|_| KvsError::InternalError)?;
        let engine = SledKvsEngine { db };
        Ok(engine)
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let ret = self
            .db
            .insert(key.as_bytes(), value.as_bytes())
            .map_err(|_| KvsError::InternalError)?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        let ret = self
            .db
            .get(key.as_bytes())
            .map_err(|_| KvsError::InternalError)?;
        match ret {
            Some(r) => Ok(Some(must_convert_ivec_to_string(r))),
            None => Ok(None),
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let ret = self.db.remove(key).map_err(|_| KvsError::InternalError)?;
        match ret {
            Some(_) => Ok(()),
            None => Err(KvsError::KeyNotFound),
        }
    }
}

fn must_convert_ivec_to_string(v: IVec) -> String {
    let v: Arc<[u8]> = v.into();
    String::from_utf8(v.to_vec()).unwrap()
}
