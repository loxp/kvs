use clap::{App, AppSettings, Arg, SubCommand};
use kvs::{EngineType, KvStore, KvsError, KvsServer, Result};
use std::env::current_dir;
use std::path::PathBuf;
use std::process::exit;

fn main() -> Result<()> {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::DisableHelpSubcommand)
        .setting(AppSettings::SubcommandRequiredElseHelp)
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
        .value_of("ADDR")
        .ok_or(KvsError::CommandLineArgumentError)?;
    let engine_name = matches
        .value_of("ENGINE")
        .ok_or(KvsError::CommandLineArgumentError)?;

    let dir = current_dir()?;

    let engine_type = match engine_name {
        "kvs" => Ok(EngineType::Kvs(dir)),
        _ => Err(KvsError::CommandLineArgumentError),
    }?;
    let store_engine = kvs::build_engine(engine_type)?;
    let server = KvsServer::new(addr.to_string(), store_engine)?;
    server.run()?;

    Ok(())
}
