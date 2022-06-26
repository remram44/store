extern crate clap;
extern crate env_logger;
extern crate log;

use clap::{Arg, Command};
use std::borrow::Cow;
use std::env;
use std::io::Write;
use std::net::SocketAddr;
use std::path::Path;

use store::{ObjectId, PoolName};
use store::metrics::start_http_server;

fn main() {
    // Parse command line
    let mut cli = Command::new("store")
        .bin_name("store")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(
            Arg::new("verbose")
                .short('v')
                .help("Augment verbosity (print more details)")
                .multiple_occurrences(true)
        )
        .arg(
            Arg::new("serve-metrics")
                .long("serve-metrics")
                .help("Serve metrics in Prometheus format on this port")
                .takes_value(true)
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
        )
        .subcommand(Command::new("mem-store")
            .about("Start storage daemon, storing object data memory (not persistent)")
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
        )
        .subcommand(Command::new("rocksdb-store")
            .about("Start storage daemon, storing object data in rocksdb")
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
                Arg::new("dir")
                    .long("dir")
                    .help("Directory where to store object data")
                    .required(true)
                    .takes_value(true)
                    .allow_invalid_utf8(true)
            )
        )
        .subcommand(Command::new("read")
            .about("Download data as a client")
            .arg(
                Arg::new("storage-daemon")
                    .long("storage-daemon")
                    .help("Address of the storage daemon")
                    .required(true)
                    .takes_value(true)
            )
            .arg(
                Arg::new("pool")
                    .long("pool")
                    .help("Name of the pool")
                    .required(true)
                    .takes_value(true)
            )
            .arg(
                Arg::new("object-id")
                    .help("Object ID to get")
                    .required(true)
                    .takes_value(true)
            )
            .arg(
                Arg::new("offset")
                    .long("offset")
                    .help("Do a partial read starting at this byte offset")
                    .takes_value(true)
            )
            .arg(
                Arg::new("length")
                    .long("length")
                    .help("Do a partial read with this size")
                    .takes_value(true)
            )
        )
        .subcommand(Command::new("write")
            .about("Upload data as a client")
            .arg(
                Arg::new("storage-daemon")
                    .long("storage-daemon")
                    .help("Address of the storage daemon")
                    .required(true)
                    .takes_value(true)
            )
            .arg(
                Arg::new("pool")
                    .long("pool")
                    .help("Name of the pool")
                    .required(true)
                    .takes_value(true)
            )
            .arg(
                Arg::new("object-id")
                    .help("Object ID to set")
                    .required(true)
                    .takes_value(true)
            )
            .arg(
                Arg::new("data-literal")
                    .long("data-literal")
                    .help("Data to set; use either this or --data-file")
                    .takes_value(true)
            )
            .arg(
                Arg::new("data-file")
                    .long("data-file")
                    .help("Read data to set from file; use either this or --data-literal")
                    .takes_value(true)
                    .allow_invalid_utf8(true)
            )
            .arg(
                Arg::new("offset")
                    .long("offset")
                    .help("Overwrite existing object starting at this byte offset")
                    .takes_value(true)
            )
        )
        .subcommand(Command::new("delete")
            .about("Delete an object")
            .arg(
                Arg::new("storage-daemon")
                    .long("storage-daemon")
                    .help("Address of the storage daemon")
                    .required(true)
                    .takes_value(true)
            )
            .arg(
                Arg::new("pool")
                    .long("pool")
                    .help("Name of the pool")
                    .required(true)
                    .takes_value(true)
            )
            .arg(
                Arg::new("object-id")
                    .help("Object ID to set")
                    .required(true)
                    .takes_value(true)
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
        ($res:expr $(,)?) => {
            match $res {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("{}", e);
                    std::process::exit(1);
                }
            }
        };
        ($res:expr, $msg:expr $(,)?) => {
            match $res {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("{}: {}", $msg, e);
                    std::process::exit(1);
                }
            }
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
        if let Ok(val) = env::var("STORE_LOG") {
            logger_builder.parse_filters(&val);
        }
        if let Ok(val) = env::var("STORE_LOG_STYLE") {
            logger_builder.parse_write_style(&val);
        }
        logger_builder.init();
    }

    // Set up metrics
    if let Some(metrics_addr) = matches.value_of("serve-metrics") {
        let metrics_addr: SocketAddr = check!(
            metrics_addr.parse(),
            "Invalid metrics address",
        );
        start_http_server(metrics_addr);
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

            runtime
                .build()
                .unwrap()
                .block_on(run_master(
                    peer_address,
                    peer_cert,
                    peer_key,
                    peer_ca_cert,
                    listen_address,
                    listen_cert,
                    listen_key,
                ))
                .unwrap();
        }
        Some("mem-store") => {
            use store::daemon::run_storage_daemon;
            use store::storage::mem_store::create_mem_store;

            let s_matches = matches.subcommand_matches("mem-store").unwrap();
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
            let (storage_backend, device_id) = create_mem_store();

            runtime
                .build()
                .unwrap()
                .block_on(run_storage_daemon(
                    peer_address,
                    peer_cert,
                    peer_key,
                    peer_ca_cert,
                    listen_address,
                    Box::new(storage_backend),
                    device_id,
                ))
                .unwrap();
        }
        #[cfg(feature = "rocksdb")]
        Some("rocksdb-store") => {
            use store::daemon::run_storage_daemon;
            use store::storage::rocksdb_store::create_rocksdb_store;

            let s_matches = matches.subcommand_matches("rocksdb-store").unwrap();
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
            let listen_address: SocketAddr =
                check!(listen_address.parse(), "Invalid listen-address",);
            let storage_dir = s_matches.value_of_os("dir").unwrap();
            let storage_dir = Path::new(storage_dir);
            let (storage_backend, device_id) = check!(create_rocksdb_store(storage_dir));

            runtime
                .build()
                .unwrap()
                .block_on(run_storage_daemon(
                    peer_address,
                    peer_cert,
                    peer_key,
                    peer_ca_cert,
                    listen_address,
                    Box::new(storage_backend),
                    device_id,
                ))
                .unwrap();
        }
        #[cfg(not(feature = "rocksdb"))]
        Some("rocksdb-store") => {
            eprintln!("RocksDB support was not compiled in");
            std::process::exit(1);
        }
        Some("read") => {
            use store::client::create_client;

            let s_matches = matches.subcommand_matches("read").unwrap();
            let storage_daemon_address = s_matches.value_of("storage-daemon").unwrap();
            let storage_daemon_address: SocketAddr = check!(
                storage_daemon_address.parse(),
                "Invalid storage-daemon address",
            );
            let pool = s_matches.value_of("pool").unwrap();
            let object_id = s_matches.value_of("object-id").unwrap();
            let object_id = ObjectId(object_id.as_bytes().to_owned());
            let offset: Option<u32> = match s_matches.value_of("offset") {
                None => None,
                Some(s) => match s.parse() {
                    Ok(i) => Some(i),
                    Err(_) => {
                        eprintln!("Invalid offset");
                        std::process::exit(2);
                    }
                },
            };
            let length: Option<u32> = match s_matches.value_of("length") {
                None => None,
                Some(s) => match s.parse() {
                    Ok(i) => Some(i),
                    Err(_) => {
                        eprintln!("Invalid length");
                        std::process::exit(2);
                    }
                },
            };

            runtime
                .build()
                .unwrap()
                .block_on(async move {
                    let client =
                        create_client(storage_daemon_address, PoolName(pool.to_owned())).await?;
                    let data = match (offset, length) {
                        (None, None) => client.read_object(&object_id).await?,
                        (offset, length) => {
                            client
                                .read_part(
                                    &object_id,
                                    offset.unwrap_or(0),
                                    length.unwrap_or(u32::MAX),
                                )
                                .await?
                        }
                    };
                    match data {
                        None => eprintln!("No such key"),
                        Some(bytes) => std::io::stdout().write_all(&bytes).unwrap(),
                    }
                    Ok(()) as Result<(), Box<dyn std::error::Error>>
                })
                .unwrap();
        }
        Some("write") => {
            use store::client::create_client;

            let s_matches = matches.subcommand_matches("write").unwrap();
            let storage_daemon_address = s_matches.value_of("storage-daemon").unwrap();
            let storage_daemon_address: SocketAddr = check!(
                storage_daemon_address.parse(),
                "Invalid storage-daemon address",
            );
            let pool = s_matches.value_of("pool").unwrap();
            let object_id = s_matches.value_of("object-id").unwrap();
            let object_id = ObjectId(object_id.as_bytes().to_owned());
            let offset: Option<u32> = match s_matches.value_of("offset") {
                None => None,
                Some(s) => match s.parse() {
                    Ok(i) => Some(i),
                    Err(_) => {
                        eprintln!("Invalid offset");
                        std::process::exit(2);
                    }
                },
            };
            let data: Cow<[u8]> = {
                let data_literal = s_matches.value_of("data-literal");
                let data_file = s_matches.value_of_os("data-file");
                if data_literal.is_some() && data_file.is_some() {
                    eprintln!("Please provide EITHER --data-literal or --data-file");
                    cli.find_subcommand_mut("write")
                        .unwrap()
                        .print_help()
                        .expect("Can't print help");
                    std::process::exit(2);
                } else if let Some(d) = data_literal {
                    Cow::Borrowed(d.as_bytes())
                } else if let Some(path) = data_file {
                    fn read_file(path: &Path) -> Result<Vec<u8>, std::io::Error> {
                        use std::io::Read;
                        let mut file = std::fs::File::open(path)?;
                        let mut data = Vec::new();
                        file.read_to_end(&mut data)?;
                        Ok(data)
                    }

                    match read_file(Path::new(path)) {
                        Ok(d) => Cow::Owned(d),
                        Err(e) => {
                            eprintln!("Error reading data file: {}", e);
                            std::process::exit(1);
                        }
                    }
                } else {
                    eprintln!("Data missing, please provide --data-literal or --data-file");
                    cli.find_subcommand_mut("write")
                        .unwrap()
                        .print_help()
                        .expect("Can't print help");
                    std::process::exit(2);
                }
            };

            runtime
                .build()
                .unwrap()
                .block_on(async move {
                    let client = create_client(
                        storage_daemon_address,
                        PoolName(pool.to_owned()),
                    ).await?;
                    match offset {
                        None => client.write_object(&object_id, &data).await?,
                        Some(offset) => client.write_part(&object_id, offset, &data).await?,
                    }
                    Ok(()) as Result<(), Box<dyn std::error::Error>>
                })
                .unwrap();
        }
        Some("delete") => {
            use store::client::create_client;

            let s_matches = matches.subcommand_matches("delete").unwrap();
            let storage_daemon_address = s_matches.value_of("storage-daemon").unwrap();
            let storage_daemon_address: SocketAddr = check!(
                storage_daemon_address.parse(),
                "Invalid storage-daemon address",
            );
            let pool = s_matches.value_of("pool").unwrap();
            let object_id = s_matches.value_of("object-id").unwrap();
            let object_id = ObjectId(object_id.as_bytes().to_owned());

            runtime
                .build()
                .unwrap()
                .block_on(async move {
                    let client = create_client(
                        storage_daemon_address,
                        PoolName(pool.to_owned()),
                    ).await?;
                    client.delete_object(&object_id).await?;
                    Ok(()) as Result<(), Box<dyn std::error::Error>>
                })
                .unwrap();
        }
        _ => {
            cli.print_help().expect("Can't print help");
            std::process::exit(2);
        }
    }
}
