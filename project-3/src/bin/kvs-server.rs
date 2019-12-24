use clap::{App, AppSettings, Arg, SubCommand};
use kvs::{KvsError, KvsServer, Result};
use kvs::engine::{EngineType, KvStore, SledKvsEngine};
use std::env::current_dir;
use std::path::PathBuf;
use std::process::exit;

fn main() -> Result<()> {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::DisableHelpSubcommand)
        .setting(AppSettings::VersionlessSubcommands)
        .arg(
            Arg::with_name("addr")
                .long("addr")
                .value_name("ADDR")
                .help("address")
                .default_value("127.0.0.1:4000"),
        )
        .arg(
            Arg::with_name("engine")
                .long("engine")
                .value_name("ENGINE")
                .help("store engine, currently support kvs, sled")
                .default_value("kvs"),
        )
        .get_matches();

    let addr = matches
        .value_of("addr")
        .ok_or(KvsError::CommandLineArgumentError)?;
    let engine_name = matches
        .value_of("engine")
        .ok_or(KvsError::CommandLineArgumentError)?;

    let dir = current_dir()?;

    let engine_type = match engine_name {
        "kvs" => Ok(EngineType::Kvs(dir)),
        "sled" => Ok(EngineType::Sled(dir)),
        _ => Err(KvsError::InvalidStorageEngineType),
    }?;

    engine_type.check()?;

    match engine_type {
        EngineType::Kvs(path) => {
            let engine = KvStore::open(path)?;
            let server = KvsServer::new(addr.to_string(), engine)?;
            server.run()
        }
        EngineType::Sled(path) => {
            let engine = SledKvsEngine::open(path)?;
            let server = KvsServer::new(addr.to_string(), engine)?;
            server.run()
        }
    }
}
