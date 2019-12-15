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
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::Io(err)
    }
}
