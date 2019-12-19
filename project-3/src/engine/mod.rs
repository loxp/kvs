use crate::Result;

/// Store engine abstraction of kvs
pub trait KvsEngine {
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
