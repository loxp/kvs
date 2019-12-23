use failure::Fail;
use std::io;

/// Error type for kvs
#[derive(Fail, Debug)]
pub enum KvsError {
    /// key not found error
    #[fail(display = "Key not found")]
    KeyNotFound,
    /// IO error
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    /// File not found error
    #[fail(display = "File not found")]
    FileNotFound,
    /// Serde json error
    #[fail(display = "{}", _0)]
    SerdeJson(#[cause] serde_json::Error),
    /// Internel error
    #[fail(display = "Internal error")]
    InternalError,
    /// Command line argument error
    #[fail(display = "Command line argument error")]
    CommandLineArgumentError,
    /// Invalid server response
    #[fail(display = "Invalid server response")]
    InvalidServerResponse,
    /// Invalid request
    #[fail(display = "Invalid request")]
    InvalidRequest,
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::Io(err)
    }
}

impl From<serde_json::error::Error> for KvsError {
    fn from(err: serde_json::error::Error) -> KvsError {
        KvsError::SerdeJson(err)
    }
}
