use crate::KvsError;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const DEFAULT_FILE_CAPACITY: u64 = 1024;

/// key value store
pub struct KvStore {
    file_store: FileStore,
    index: BTreeMap<String, CommandPosition>,
}

#[derive(Serialize, Deserialize, Debug)]
/// Operation command enum
pub enum Command {
    /// Set command
    Set {
        /// key of set command
        key: String,
        /// value of set command
        value: String,
    },
    /// Del command
    Del {
        /// key of del command
        key: String,
    },
}

struct FileStore {
    dir: PathBuf,
    current_file_num: u64,
    current_write_log: WalWriter<File>,
    read_logs: Vec<WalReader<File>>,
}

struct WalWriter<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

struct WalReader<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

#[derive(Debug)]
struct CommandPosition {
    pub file_num: u64,
    pub pos: u64,
    pub len: u64,
}

/// Result type for kvs
pub type Result<T> = std::result::Result<T, KvsError>;

impl KvStore {
    /// open and create KvStore
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut file_store = FileStore::open(path.as_ref().to_path_buf())?;
        let mut index = BTreeMap::new();

        Self::load(&mut file_store, &mut index)?;

        Ok(Self { file_store, index })
    }

    /// set a key value pair
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::set(key.clone(), value);
        let cmd_pos = self.file_store.write_command(cmd)?;
        self.index.insert(key, cmd_pos);
        Ok(())
    }

    /// get value by key
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let cmd_pos = self.index.get(&key);
        if let None = cmd_pos {
            return Ok(None);
        }
        let cmd = self.file_store.read_command_position(cmd_pos.unwrap())?;
        match cmd {
            Command::Set { key: _, value: v } => Ok(Some(v)),
            _ => Err(KvsError::InternalError),
        }
    }

    /// remove a key value pair by key
    pub fn remove(&mut self, key: String) -> Result<()> {
        if let None = self.index.get(&key) {
            return Err(KvsError::KeyNotFound);
        }
        let cmd = Command::del(key.clone());
        let _cmd_pos = self.file_store.write_command(cmd)?;
        self.index.remove(&key);
        Ok(())
    }

    fn load(
        file_store: &mut FileStore,
        index: &mut BTreeMap<String, CommandPosition>,
    ) -> Result<()> {
        for i in 0..file_store.read_logs.len() {
            let reader = file_store
                .read_logs
                .get_mut(i)
                .ok_or_else(|| KvsError::FileNotFound)?;
            let mut pos = reader.seek(SeekFrom::Start(0))?;
            let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
            while let Some(cmd) = stream.next() {
                let new_pos = stream.byte_offset();
                match cmd? {
                    Command::Set { key, .. } => {
                        let cmd_pos = CommandPosition {
                            file_num: i as u64,
                            pos,
                            len: new_pos as u64 - pos,
                        };
                        index.insert(key, cmd_pos);
                    }
                    Command::Del { key } => {
                        index.remove(&key);
                    }
                }
                pos = new_pos as u64;
            }
        }

        Ok(())
    }
}

impl FileStore {
    pub fn open(path: PathBuf) -> Result<FileStore> {
        fs::create_dir_all(&path)?;

        let mut sorted_file_number_list = Self::get_sorted_file_number_list(&path)?;
        if sorted_file_number_list.is_empty() {
            let mut readers: Vec<WalReader<File>> = Vec::new();
            let writer = Self::build_wal_writer(&path, 0)?;
            let wal_path = Self::wal_path(&path, 0);
            let read_wal = File::open(wal_path)?;
            let reader = WalReader::new(read_wal)?;
            readers.push(reader);
            Ok(FileStore {
                dir: path.clone(),
                current_file_num: 0,
                current_write_log: writer,
                read_logs: readers,
            })
        } else {
            // take out the last file, and put all other files into reader list
            let last_file_num = sorted_file_number_list.pop().unwrap();
            let mut readers: Vec<WalReader<File>> = Vec::new();
            for file_num in sorted_file_number_list.iter() {
                let wal_path = Self::wal_path(&path, *file_num);
                let read_wal = File::open(wal_path)?;
                let reader = WalReader::new(read_wal)?;
                readers.push(reader);
            }

            let wal_path = Self::wal_path(&path, last_file_num);
            let read_wal = File::open(wal_path)?;
            let reader = WalReader::new(read_wal)?;
            readers.push(reader);

            let writer = Self::build_wal_writer(&path, last_file_num)?;
            Ok(FileStore {
                dir: path.clone(),
                current_file_num: last_file_num,
                current_write_log: writer,
                read_logs: readers,
            })
        }
    }

    fn write_command(&mut self, cmd: Command) -> Result<CommandPosition> {
        if self.current_write_log.is_full() {
            self.change_to_new_wal()?;
        }

        let data = serde_json::to_vec(&cmd)?;
        let pos = self.current_write_log.pos;
        self.current_write_log.write(&data)?;
        self.current_write_log.flush()?; // important, the reader may not read the correct data if not flush.

        Ok(CommandPosition {
            file_num: self.current_file_num,
            pos,
            len: data.len() as u64,
        })
    }

    fn read_command_position(&mut self, cmd_pos: &CommandPosition) -> Result<Command> {
        let wal_reader = self
            .read_logs
            .get_mut(cmd_pos.file_num as usize)
            .ok_or_else(|| KvsError::KeyNotFound)?;

        wal_reader.seek(SeekFrom::Start(cmd_pos.pos))?;

        // cannot use Vec::with_capacity(), since the len() is 0
        let mut data = vec![0; cmd_pos.len as usize];
        let _read_size = wal_reader.read(data.as_mut_slice())?;
        let cmd = serde_json::from_slice::<Command>(&data)?;

        // TODO: use take to reduce copy?
        //         let cmd_reader = wal_reader.take(cmd_pos.len);
        //        let cmd = serde_json::from_reader(cmd_reader)?;
        Ok(cmd)
    }

    fn build_wal_writer(path: &Path, file_num: u64) -> Result<WalWriter<File>> {
        let path = Self::wal_path(&path, file_num);
        let file = Self::new_wal_file(path)?;
        let writer = WalWriter::new(file)?;
        Ok(writer)
    }

    fn get_sorted_file_number_list(path: &Path) -> Result<Vec<u64>> {
        let mut file_number_list: Vec<u64> = fs::read_dir(path)?
            .flat_map(|res| -> Result<_> { Ok(res?.path()) })
            .filter(|path| Self::is_wal_file(path))
            .flat_map(|path| {
                path.file_name()
                    .and_then(OsStr::to_str)
                    .map(|s| s.trim_end_matches(".wal"))
                    .map(str::parse::<u64>)
            })
            .flatten()
            .collect();
        file_number_list.sort_unstable();
        Ok(file_number_list)
    }

    fn change_to_new_wal(&mut self) -> Result<()> {
        let current_num = self.current_file_num + 1;
        self.current_write_log = Self::build_wal_writer(&self.dir, current_num)?;
        self.current_file_num = current_num;
        let wal_path = Self::wal_path(&self.dir, current_num);
        let read_wal = File::open(wal_path)?;
        let reader = WalReader::new(read_wal)?;
        self.read_logs.push(reader);
        Ok(())
    }

    fn is_wal_file(path: &Path) -> bool {
        path.is_file() && path.starts_with("kvs_") && path.ends_with(".wal")
    }

    fn wal_path(path: &Path, file_number: u64) -> PathBuf {
        path.join(format!("kvs_{}.wal", file_number))
    }

    fn new_wal_file(path: PathBuf) -> Result<File> {
        let result = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(path)?;
        Ok(result)
    }
}

impl<W: Write + Seek> WalWriter<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::End(0))?;
        Ok(WalWriter {
            writer: BufWriter::new(inner),
            pos,
        })
    }

    fn is_full(&self) -> bool {
        self.pos >= DEFAULT_FILE_CAPACITY
    }
}

impl<W: Write + Seek> Write for WalWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let length = self.writer.write(buf)?;
        self.pos += length as u64;
        Ok(length)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for WalWriter<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}

impl<R: Read + Seek> WalReader<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(WalReader {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for WalReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for WalReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

impl Command {
    /// create a set command
    pub fn set(key: String, value: String) -> Self {
        Self::Set { key, value }
    }

    /// create a del command
    pub fn del(key: String) -> Self {
        Self::Del { key }
    }
}
