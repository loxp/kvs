use crate::KvsError;

/// Result type for kvs
pub type Result<T> = std::result::Result<T, KvsError>;
