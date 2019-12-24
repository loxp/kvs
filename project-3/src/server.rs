use crate::{codec, engine, EngineType, KvStore, KvsEngine, KvsError, Result, SledKvsEngine};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

/// Network Server of kvs
pub struct KvsServer<K: KvsEngine> {
    addr: String,
    engine: Arc<Mutex<K>>,
}

impl<K: KvsEngine + 'static> KvsServer<K> {
    /// Constructor of KvsServer
    pub fn new(addr: String, engine: K) -> Result<Self> {
        let engine = Arc::new(Mutex::new(engine));
        let server = KvsServer { addr, engine };
        Ok(server)
    }

    /// Run the KvsServer
    pub fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(self.addr.clone())?;

        eprintln!(
            "kvs server {}, listening on {}, start success!",
            env!("CARGO_PKG_VERSION"),
            self.addr.clone()
        );

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let engine = self.engine.clone();
                    thread::spawn(move || handle_stream(stream, engine));
                }
                Err(e) => {
                    println!("connection failed: {:?}", e);
                }
            }
        }

        Ok(())
    }
}

pub fn handle_stream<K: KvsEngine>(stream: TcpStream, engine: Arc<Mutex<K>>) -> Result<()> {
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    loop {
        let mut read_line = String::new();
        reader.read_line(&mut read_line)?;
        let msg = codec::decode(read_line)?;

        if msg.len() <= 1 {
            let err_resp = format!("{}\n", KvsError::InvalidRequest);
            writer.write(err_resp.as_bytes())?;
            writer.flush();
            continue;
        }

        match msg.get(0).ok_or(KvsError::InvalidRequest)?.as_ref() {
            "get" => {
                let key = msg.get(1).ok_or(KvsError::InvalidRequest)?;
                let value = engine.lock().unwrap().get(key.to_string())?;
                match value {
                    Some(v) => {
                        let resp = format!("{}\n", v);
                        writer.write(resp.as_bytes())?;
                        writer.flush();
                    }
                    None => {
                        let err_resp = format!("{}\n", KvsError::KeyNotFound);
                        writer.write(err_resp.as_bytes())?;
                        writer.flush();
                    }
                }
            }
            "set" => {
                let key = msg.get(1).ok_or(KvsError::InvalidRequest)?;
                let value = msg.get(2).ok_or(KvsError::InvalidRequest)?;
                engine
                    .lock()
                    .unwrap()
                    .set(key.to_string(), value.to_string())?;
                writer.write("OK\n".as_bytes())?;
                writer.flush()?;
            }
            "rm" => {
                let key = msg.get(1).ok_or(KvsError::InvalidRequest)?;
                let ret = engine.lock().unwrap().remove(key.to_string());
                match ret {
                    Ok(()) => {
                        writer.write("OK\n".as_bytes())?;
                        writer.flush()?;
                    }
                    Err(e) => {
                        writer.write(format!("{}\n", e).as_bytes())?;
                        writer.flush()?;
                    }
                }
            }
            _ => {
                let err_resp = format!("{}\n", KvsError::InvalidRequest);
                writer.write(err_resp.as_bytes())?;
                writer.flush();
            }
        }
    }
}
