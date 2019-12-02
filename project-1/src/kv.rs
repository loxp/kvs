use std::collections::HashMap;

/// key value store
pub struct KvStore {
    store: HashMap<String, String>,
}

impl KvStore {
    /// constructor of KvStore
    pub fn new() -> Self {
        KvStore {
            store: HashMap::new(),
        }
    }

    /// set a key value pair
    pub fn set(&mut self, key: String, value: String) {
        self.store.insert(key, value);
    }

    /// get value by key
    pub fn get(&self, key: String) -> Option<String> {
        self.store.get(&key).and_then(|v| Option::from(String::from(v)))
    }

    /// remove a key value pair by key
    pub fn remove(&mut self, key: String) {
        self.store.remove(&key);
    }
}