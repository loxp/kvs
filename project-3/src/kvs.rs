use crate::KvsError;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const DEFAULT_FILE_CAPACITY: u64 = 8192;
const DEFAULT_COMPACT_COUNT: u64 = 1000;

/// key value store
pub struct KvStore {
    file_store: FileStore,
    index: BTreeMap<String, CommandPosition>,
    compact_counter: AtomicU64,
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
    read_logs: HashMap<u64, WalReader<File>>,
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
        let compact_counter = AtomicU64::new(0);
        Self::load(&mut file_store, &mut index)?;
        Self::compact(&mut file_store, &mut index)?;

        Ok(Self {
            file_store,
            index,
            compact_counter,
        })
    }

    /// set a key value pair
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::set(key.clone(), value);
        let cmd_pos = self.file_store.write_command(cmd)?;
        self.index.insert(key, cmd_pos);
        self.compact_counter.fetch_add(1, Ordering::Relaxed);
        if DEFAULT_COMPACT_COUNT
            == self
                .compact_counter
                .compare_and_swap(DEFAULT_COMPACT_COUNT, 0, Ordering::SeqCst)
        {
            Self::compact(&mut self.file_store, &mut self.index)?;
        }
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
        self.compact_counter.fetch_add(1, Ordering::Relaxed);
        if DEFAULT_COMPACT_COUNT
            == self
                .compact_counter
                .compare_and_swap(DEFAULT_COMPACT_COUNT, 0, Ordering::SeqCst)
        {
            Self::compact(&mut self.file_store, &mut self.index)?;
        }
        Ok(())
    }

    fn load(
        file_store: &mut FileStore,
        index: &mut BTreeMap<String, CommandPosition>,
    ) -> Result<()> {
        let start_file_num = file_store.current_file_num + 1 - file_store.read_logs.len() as u64;

        for i in start_file_num..file_store.current_file_num + 1 {
            let reader = file_store.read_logs.get_mut(&(i as u64));
            if reader.is_none() {
                continue;
            }
            let reader = reader.unwrap();
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

    // compact the file and update the index
    fn compact(
        file_store: &mut FileStore,
        index: &mut BTreeMap<String, CommandPosition>,
    ) -> Result<()> {
        let mut read_logs_new: HashMap<u64, WalReader<File>> =
            HashMap::with_capacity(file_store.read_logs.len());

        // do not compact the current log
        let start_file_num = file_store.current_file_num + 1 - file_store.read_logs.len() as u64;
        for i in start_file_num..file_store.current_file_num {
            let reader = file_store.read_logs.get_mut(&(i as u64));
            if reader.is_none() {
                continue;
            }
            let reader = reader.unwrap();
            let mut pos = reader.seek(SeekFrom::Start(0))?;
            let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();

            let file_path_new: PathBuf = file_store.dir.join(format!("kvs_{}.wal.new", i));
            let log_new = FileStore::new_wal_file(file_path_new.clone())?;
            let mut writer_new = WalWriter::new(log_new)?;
            let mut position_new: u64 = 0;

            while let Some(cmd) = stream.next() {
                let new_pos = stream.byte_offset();
                let cmd = cmd?;
                let key_ref = cmd.get_key();
                if let Some(position_in_index) = index.get(key_ref) {
                    let &CommandPosition {
                        file_num: current_position_file_num,
                        pos: current_position_offset,
                        ..
                    } = position_in_index;
                    if current_position_file_num != i as u64 || current_position_offset != pos {
                        // this is an out of date command, and need to be dropped,
                        // do nothing to let it drop
                    } else if current_position_file_num == i as u64 {
                        // this is an up to date command in current read log, and need to be copied
                        // to the new read log, then update the index.
                        let cmd_to_write = serde_json::to_vec(&cmd)?;
                        let cmd_pos_len = writer_new.write(&cmd_to_write)?;
                        writer_new.flush()?;
                        let cmd_pos_new = CommandPosition {
                            file_num: i as u64,
                            pos: position_new,
                            len: cmd_pos_len as u64,
                        };
                        index.insert(key_ref.to_string(), cmd_pos_new);
                        position_new += cmd_pos_len as u64;
                    }
                }
                pos = new_pos as u64;
            }

            // replace the origin file
            let file_path_origin: PathBuf = FileStore::wal_path(&file_store.dir, i as u64);
            fs::rename(file_path_new.clone(), file_path_origin.clone())?;

            if fs::metadata(file_path_origin.clone())?.len() == 0 {
                fs::remove_file(file_path_origin.clone())?;
                read_logs_new.remove(&(i as u64));
            } else {
                let read_log_file_new = File::open(file_path_origin)?;
                let reader_new = WalReader::new(read_log_file_new)?;
                read_logs_new.insert(i as u64, reader_new);
            }
        }

        // move the current read log to the new read logs
        let (current_file_num, last_read_log) = file_store
            .read_logs
            .remove_entry(&file_store.current_file_num)
            .unwrap();
        mem::replace(&mut file_store.read_logs, read_logs_new);
        file_store.read_logs.insert(current_file_num, last_read_log);

        Ok(())
    }
}

impl FileStore {
    pub fn open(path: PathBuf) -> Result<FileStore> {
        fs::create_dir_all(&path)?;

        let mut sorted_file_number_list = Self::get_sorted_file_number_list(&path)?;
        if sorted_file_number_list.is_empty() {
            let mut readers: HashMap<u64, WalReader<File>> = HashMap::new();
            let writer = Self::build_wal_writer(&path, 0)?;
            let wal_path = Self::wal_path(&path, 0);
            let read_wal = File::open(wal_path)?;
            let reader = WalReader::new(read_wal)?;
            readers.insert(0, reader);
            Ok(FileStore {
                dir: path.clone(),
                current_file_num: 0,
                current_write_log: writer,
                read_logs: readers,
            })
        } else {
            // take out the last file, and put all other files into reader list
            let last_file_num = sorted_file_number_list.pop().unwrap();
            let mut readers: HashMap<u64, WalReader<File>> = HashMap::new();
            for file_num in sorted_file_number_list.iter() {
                let wal_path = Self::wal_path(&path, *file_num);
                let read_wal = File::open(wal_path)?;
                let reader = WalReader::new(read_wal)?;
                readers.insert(file_num.clone(), reader);
            }

            let wal_path = Self::wal_path(&path, last_file_num);
            let read_wal = File::open(wal_path)?;
            let reader = WalReader::new(read_wal)?;
            readers.insert(last_file_num, reader);

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
            .get_mut(&cmd_pos.file_num)
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
                path.file_stem()
                    .and_then(OsStr::to_str)
                    .map(|s| s.trim_start_matches("kvs_"))
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
        self.read_logs.insert(current_num, reader);
        Ok(())
    }

    fn is_wal_file(path: &Path) -> bool {
        path.is_file()
            && path
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("kvs_")
            && path.extension().unwrap().to_str().unwrap() == "wal"
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

    /// get the key of command
    pub fn get_key(&self) -> &String {
        match self {
            Command::Set { key, .. } => &key,
            Command::Del { key } => &key,
        }
    }
}
