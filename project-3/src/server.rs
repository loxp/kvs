use crate::codec;
use crate::{KvsEngine, KvsError, Result};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

/// Network Server of kvs
pub struct KvsServer<K: KvsEngine + Sync> {
    addr: String,
    engine: K,
}

impl<K: KvsEngine + Sync> KvsServer<K> {
    /// Constructor of KvsServer
    pub fn new(addr: String, engine: K) -> Result<Self> {
        let server = KvsServer { addr, engine };
        Ok(server)
    }

    /// Run the KvsServer
    pub fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(self.addr.clone())?;
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    thread::spawn(|| handle_stream(stream, self.engine));
                }
                Err(e) => {
                    println!("connection failed: {:?}", e);
                }
            }
        }

        Ok(())
    }
}

pub fn handle_stream<K: KvsEngine>(stream: TcpStream, mut engine: K) -> Result<()> {
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    loop {
        let mut read_line = String::new();
        reader.read_line(&mut read_line)?;
        let msg = codec::decode(read_line)?;

        if msg.len() <= 1 {
            let err_resp = format!("{:?}\n", KvsError::InvalidRequest);
            writer.write(err_resp.as_bytes())?;
            continue;
        }

        match msg.get(0).ok_or(KvsError::InvalidRequest)?.as_ref() {
            "get" => {
                let key = msg.get(1).ok_or(KvsError::InvalidRequest)?;
                let value = engine.get(key.to_string())?;
                match value {
                    Some(v) => {
                        let resp = format!("{:?}\n", v);
                        writer.write(resp.as_bytes())?;
                    }
                    None => {
                        let err_resp = format!("{:?}\n", KvsError::InvalidRequest);
                        writer.write(err_resp.as_bytes())?;
                    }
                }
            }
            "set" => {
                let key = msg.get(1).ok_or(KvsError::InvalidRequest)?;
                let value = msg.get(2).ok_or(KvsError::InvalidRequest)?;
                engine.set(key.to_string(), value.to_string())?;
            }
            "rm" => {
                let key = msg.get(1).ok_or(KvsError::InvalidRequest)?;
                engine.remove(key.to_string())?;
            }
            _ => {
                let err_resp = format!("{:?}\n", KvsError::InvalidRequest).as_bytes();
                writer.write(err_resp)?;
            }
        }
    }
}
