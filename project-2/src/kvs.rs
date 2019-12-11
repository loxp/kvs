use std::path::Path;
use std::collections::HashMap;
use super::error::KvsError;

/// key value store
pub struct KvStore {
    store: HashMap<String, String>,
}

/// Result type for kvs
pub type Result<T> = std::result::Result<T, KvsError>;

impl KvStore {
    /// constructor of KvStore
    pub fn new() -> Self {
        KvStore {
            store: HashMap::new(),
        }
    }

    /// open and create KvStore
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let store = KvStore{
            store: HashMap::new(),
        };
        Ok(store)
    }
    

    /// set a key value pair
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.store.insert(key, value);
        Ok(())
    }

    /// get value by key
    pub fn get(&self, key: String) -> Result<Option<String>> {
        let ret = self.store.get(&key).and_then(|v| Option::from(String::from(v)));
        Ok(ret)
    }

    /// remove a key value pair by key
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.store.remove(&key);
        Ok(())
    }
}