use crate::codec::{decode, encode, Message};
use crate::error::KvsError::InternalError;
use crate::{KvsError, Result};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::net::TcpStream;

/// client of kvs
pub struct KvsClient<'a> {
    //    conn: TcpStream,
    reader: BufReader<&'a TcpStream>,
    writer: BufWriter<&'a TcpStream>,
}

impl<'a> KvsClient<'a> {
    /// Create a new kvs client with network address
    pub fn new(stream: &'a TcpStream) -> Result<Self> {
        let reader = BufReader::new(stream);
        let writer = BufWriter::new(stream);
        let client = KvsClient { reader, writer };
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
                r => Err(KvsError::InvalidServerResponse),
            },
            None => Err(KvsError::InvalidServerResponse),
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
        let req = vec!["rm".to_string(), key];
        let ret = self.write_request_and_get_result(req)?;
        match ret {
            Some(msg) => match msg.as_str() {
                "OK" => Ok(()),
                // TODO: deserilize error from string
                r => Err(KvsError::KeyNotFound),
            },
            None => Err(KvsError::InvalidServerResponse),
        }
    }

    fn write_request_and_get_result(&mut self, msg: Message) -> Result<Option<String>> {
        let write_line = encode(msg)?;
        self.writer.write(write_line.as_bytes())?;
        self.writer.flush()?;
        let mut read_line = String::new();
        let len = self.reader.read_line(&mut read_line)?;
        match len {
            0 => Ok(None),
            _ => Ok(Some(read_line.trim().to_string())),
        }
    }
}
