extern crate clap;
extern crate env_logger;
extern crate log;

use clap::{Arg, Command};
use std::env;
use std::net::SocketAddr;
use std::path::Path;

fn main() {
    // Parse command line
    let mut cli = Command::new("store")
        .bin_name("store")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(Arg::new("verbose")
            .short('v')
            .help("Augment verbosity (print more details)")
            .multiple_occurrences(true)
        )
        .subcommand(Command::new("master")
            .about("Start master server, used for coordination and authentication")
            .arg(
                Arg::new("peer-address")
                    .long("peer-address")
                    .help("Address to listen on for storage daemons")
                    .required(true)
                    .takes_value(true)
            )
            .arg(
                Arg::new("peer-cert")
                    .long("peer-cert")
                    .help("Path to certificate to present for peer connections")
                    .required(true)
                    .takes_value(true)
                    .allow_invalid_utf8(true)
            )
            .arg(
                Arg::new("peer-key")
                    .long("peer-key")
                    .help("Path to key for peer-cert")
                    .required(true)
                    .takes_value(true)
                    .allow_invalid_utf8(true)
            )
            .arg(
                Arg::new("peer-ca-cert")
                    .long("peer-ca-cert")
                    .help("Path to certificate to use to validate peer connections")
                    .required(true)
                    .takes_value(true)
                    .allow_invalid_utf8(true)
            )
            .arg(
                Arg::new("listen-address")
                    .long("listen-address")
                    .help("Address to listen on for clients")
                    .required(true)
                    .takes_value(true)
            )
            .arg(
                Arg::new("listen-cert")
                    .long("listen-cert")
                    .help("Path to certificate presented to clients")
                    .required(true)
                    .takes_value(true)
                    .allow_invalid_utf8(true)
            )
            .arg(
                Arg::new("listen-key")
                    .long("listen-key")
                    .help("Path to key for listen-cert")
                    .required(true)
                    .takes_value(true)
                    .allow_invalid_utf8(true)
            )
        );

    let matches = match cli.try_get_matches_from_mut(env::args_os()) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("{}", e);
            std::process::exit(2);
        }
    };

    macro_rules! check {
        ($res:expr, $msg:expr,) => {
            match $res {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("{}: {}", $msg, e);
                    std::process::exit(1);
                }
            }
        };
        ($res:expr, $msg:expr) => {
            check!($res, $msg,)
        };
    }

    // Set up logging
    {
        let level = match matches.occurrences_of("verbose") {
            0 => log::LevelFilter::Warn,
            1 => log::LevelFilter::Info,
            2 => log::LevelFilter::Debug,
            _ => log::LevelFilter::Trace,
        };
        let mut logger_builder = env_logger::builder();
        logger_builder.filter(None, level);
        if let Ok(val) = env::var("DOUBLEGIT_LOG") {
            logger_builder.parse_filters(&val);
        }
        if let Ok(val) = env::var("DOUBLEGIT_LOG_STYLE") {
            logger_builder.parse_write_style(&val);
        }
        logger_builder.init();
    }

    let mut runtime = tokio::runtime::Builder::new_current_thread();
    runtime.enable_all();

    match matches.subcommand_name() {
        Some("master") => {
            use store::master::run_master;

            let s_matches = matches.subcommand_matches("master").unwrap();
            let peer_address = s_matches.value_of("peer-address").unwrap();
            let peer_address: SocketAddr = check!(
                peer_address.parse(),
                "Invalid peer-address",
            );
            let peer_cert = s_matches.value_of_os("peer-cert").unwrap();
            let peer_cert = Path::new(peer_cert);
            let peer_key = s_matches.value_of_os("peer-key").unwrap();
            let peer_key = Path::new(peer_key);
            let peer_ca_cert = s_matches.value_of_os("peer-ca-cert").unwrap();
            let peer_ca_cert = Path::new(peer_ca_cert);
            let listen_address = s_matches.value_of("listen-address").unwrap();
            let listen_address: SocketAddr = check!(
                listen_address.parse(),
                "Invalid listen-address",
            );
            let listen_cert = s_matches.value_of_os("listen-cert").unwrap();
            let listen_cert = Path::new(listen_cert);
            let listen_key = s_matches.value_of_os("listen-key").unwrap();
            let listen_key = Path::new(listen_key);

            runtime.build().unwrap().block_on(
                run_master(
                    peer_address,
                    peer_cert,
                    peer_key,
                    peer_ca_cert,
                    listen_address,
                    listen_cert,
                    listen_key,
                )
            ).unwrap();
        }
        _ => {
            cli.print_help().expect("Can't print help");
            std::process::exit(2);
        }
    }
}
