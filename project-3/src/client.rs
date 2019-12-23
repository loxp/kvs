use crate::codec::{encode, Message};
use crate::error::KvsError::InternalError;
use crate::{KvsError, Result};
use std::io::{Read, Write};
use std::net::TcpStream;

/// client of kvs
pub struct KvsClient {
    conn: TcpStream,
}

impl KvsClient {
    /// Create a new kvs client with network address
    pub fn new(addr: String) -> Result<Self> {
        let mut conn = TcpStream::connect(addr)?;
        let client = KvsClient { conn };
        Ok(client)
    }

    /// Set a key value pair.
    /// If set success, then return Ok(()),
    /// Return Err(e) when error occurs.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let req = vec!["set".to_string(), key, value];
        let ret = self.write_request_and_get_result(req)?;
        match ret {
            Some(msg) => match msg.as_str() {
                "OK" => Ok(()),
                _ => Err(KvsError::InvalidServerResponse),
            },
            _ => Err(KvsError::InvalidServerResponse),
        }
    }

    /// Get value by key
    /// If get success, return a Option.
    /// Return Err(e) when error occurs.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let req = vec!["get".to_string(), key];
        self.write_request_and_get_result(req)
    }

    /// Remove value by key
    /// If remove success, return Ok(()).
    /// Return Err(e) when error occurs.
    pub fn remove(&mut self, key: String) -> Result<()> {
        let req = vec!["remove".to_string(), key];
        let ret = self.write_request_and_get_result(req)?;
        match ret {
            Some(msg) => match msg.as_str() {
                "OK" => Ok(()),
                _ => Err(KvsError::InvalidServerResponse),
            },
            _ => Err(KvsError::InvalidServerResponse),
        }
    }

    fn write_request_and_get_result(&mut self, msg: Message) -> Result<Option<String>> {
        unimplemented!();
    }
}
