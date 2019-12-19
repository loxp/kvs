use crate::{KvsEngine, Result};

/// Network Server of kvs
pub struct KvsServer<K: KvsEngine> {
    engine: K,
}

impl<K: KvsEngine> KvsServer<K> {
    /// Constructor of KvsServer
    pub fn new(addr: String, engine: K) -> Result<Self> {
        let server = KvsServer { engine };
        Ok(server)
    }

    /// Run the KvsServer
    pub fn run(&self) -> Result<()> {
        Ok(())
    }
}
