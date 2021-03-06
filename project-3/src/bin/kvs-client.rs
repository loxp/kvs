use clap::{App, AppSettings, Arg, SubCommand};
use kvs::{KvsClient, KvsError, Result};
use std::env::current_dir;
use std::net::TcpStream;
use std::process::exit;

fn main() -> Result<()> {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::DisableHelpSubcommand)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::VersionlessSubcommands)
        .subcommand(
            SubCommand::with_name("set")
                .arg(Arg::with_name("KEY").required(true))
                .arg(Arg::with_name("VALUE").required(true))
                .arg(
                    Arg::with_name("addr")
                        .long("addr")
                        .value_name("ADDR")
                        .help("server address")
                        .default_value("127.0.0.1:4000"),
                ),
        )
        .subcommand(
            SubCommand::with_name("get")
                .arg(Arg::with_name("KEY").required(true))
                .arg(
                    Arg::with_name("addr")
                        .long("addr")
                        .value_name("ADDR")
                        .help("server address")
                        .default_value("127.0.0.1:4000"),
                ),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .arg(Arg::with_name("KEY").required(true))
                .arg(
                    Arg::with_name("addr")
                        .long("addr")
                        .value_name("ADDR")
                        .help("server address")
                        .default_value("127.0.0.1:4000"),
                ),
        )
        .get_matches();

    match matches.subcommand() {
        ("set", Some(matches)) => {
            let key = matches.value_of("KEY").expect("KEY argument missing");
            let value = matches.value_of("VALUE").expect("VALUE argument missing");
            let addr = matches.value_of("addr").expect("ADDR argument missing");
            let stream = TcpStream::connect(addr.to_string())?;
            let mut client = KvsClient::new(&stream)?;
            client.set(key.to_string(), value.to_string())?;
        }
        ("get", Some(matches)) => {
            let key = matches.value_of("KEY").expect("KEY argument missing");
            let addr = matches.value_of("addr").expect("ADDR argument missing");
            let stream = TcpStream::connect(addr.to_string())?;
            let mut client = KvsClient::new(&stream)?;
            let ret = client.get(key.to_string())?;
            match ret {
                Some(r) => {
                    println!("{}", r);
                }
                None => println!("{}", KvsError::KeyNotFound),
            }
        }
        ("rm", Some(matches)) => {
            let key = matches.value_of("KEY").expect("KEY argument missing");
            let addr = matches.value_of("addr").expect("ADDR argument missing");
            let stream = TcpStream::connect(addr.to_string())?;
            let mut client = KvsClient::new(&stream)?;
            let ret = client.remove(key.to_string());
            if let Err(e) = ret {
                eprint!("{}", e);
                exit(1);
            }
        }
        _ => unreachable!(),
    }

    Ok(())
}
