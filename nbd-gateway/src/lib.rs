mod iter;

use byteorder::{BigEndian, ReadBytesExt};
use lazy_static::lazy_static;
use log::info;
use std::io::{Cursor, Write};
use std::net::SocketAddr;
use std::sync::Mutex;

use iter::list_blocks;
use nbdkit::*;
use store::{ObjectId, PoolName};
use store::client::{Client, create_client};
use store::metrics::start_http_server;

const BLOCK_SIZE: usize = 512;

struct BlockDeviceClient {
    runtime: tokio::runtime::Runtime,
    client: Client,
    size: u64,
    base_name: Vec<u8>,
}

lazy_static! {
    static ref DEVICE: Mutex<Option<BlockDeviceClient>> = Mutex::new(None);
}

#[derive(Default)]
struct NbdGateway {
    // Box::new doesn't allocate anything unless we put some dummy
    // fields here.  In a real implementation you would put per-handle
    // data here as required.
    _not_used: i32,
}

#[derive(Default)]
struct NbdGatewayConfig {
    storage_daemon_address: Option<SocketAddr>,
    pool: Option<PoolName>,
    image: Option<Vec<u8>>,
    metrics: Option<SocketAddr>,
}

lazy_static! {
    static ref CONFIG: Mutex<NbdGatewayConfig> = Mutex::new(NbdGatewayConfig::default());
}

async fn read_image_metadata(client: &Client, base_name: &[u8]) -> Result<u64> {
    // Get metadata object
    let metadata = client.read_object(&ObjectId(base_name.to_owned())).await?;
    let metadata = metadata.ok_or(Error::new(
        libc::ENOENT,
        "No such object in storage",
    ))?;

    // Read it
    let mut metadata = Cursor::new(&metadata);
    let size = metadata.read_u64::<BigEndian>()?;

    info!("Found block device, size={}", size);
    Ok(size)
}

const CONFIG_HELP: &'static str = "\
Configuration options (pass KEY=VALUE on command line):
    storage_daemon_address: address and UDP port of the storage daemon
    pool: name of the pool
    image: base name of the block device objects in the pool
    metrics: address on which to serve metrics in Prometheus format
";

impl Server for NbdGateway {
    fn description() -> Option<&'static str> {
        Some("store gateway for Network Block Device (NBD)")
    }

    fn config_help() -> Option<&'static str> {
        Some(CONFIG_HELP)
    }

    fn name() -> &'static str {
        "store-nbd-gateway"
    }

    fn config(key: &str, value: &str) -> Result<()> {
        if key == "storage_daemon_address" {
            let addr = value.parse().map_err(|_| Error::new(libc::EINVAL, "Invalid storage daemon address"))?;
            CONFIG.lock().unwrap().storage_daemon_address = Some(addr);
        } else if key == "pool" {
            CONFIG.lock().unwrap().pool = Some(PoolName(value.to_owned()));
        } else if key == "image" {
            CONFIG.lock().unwrap().image = Some(value.as_bytes().to_owned());
        } else if key == "metrics" {
            let value = value.parse().map_err(|_| Error::new(libc::EINVAL, "Invalid address for the metrics"))?;
            CONFIG.lock().unwrap().metrics = Some(value);
        } else {
            return Err(Error::new(libc::EINVAL, format!("Invalid configuration option {}", key)));
        }
        Ok(())
    }

    fn config_complete() -> Result<()> {
        {
            let mut logger_builder = env_logger::builder();
            if let Ok(val) = std::env::var("STORE_LOG") {
                logger_builder.parse_filters(&val);
            }
            if let Ok(val) = std::env::var("STORE_LOG_STYLE") {
                logger_builder.parse_write_style(&val);
            }
            logger_builder.init();
        }

        let config = CONFIG.lock().unwrap();
        if config.storage_daemon_address.is_none() {
            Err(Error::new(libc::EINVAL, "Missing option storage_daemon_address"))
        } else if config.pool.is_none() {
            Err(Error::new(libc::EINVAL, "Missing option pool"))
        } else if config.image.is_none() {
            Err(Error::new(libc::EINVAL, "Missing option image"))
        } else {
            Ok(())
        }?;

        if let Some(addr) = config.metrics {
            start_http_server(addr);
        }

        let mut device = DEVICE.lock().unwrap();
        if device.is_none() {
            let base_name = config.image.as_ref().unwrap().clone();

            // Initialize tokio
            let mut runtime = tokio::runtime::Builder::new_current_thread();
            runtime.enable_all();
            let runtime = runtime.build().unwrap();

            // Create client
            let client = runtime.block_on(
                create_client(
                    config.storage_daemon_address.unwrap(),
                    config.pool.as_ref().unwrap().clone(),
                ),
            );
            let client = client.map_err(|e| Error::new(
                libc::EIO,
                format!("Error connecting client: {}", e),
            ))?;

            // Read size from the metadata object
            let size = runtime.block_on(read_image_metadata(&client, &base_name))
                .map_err(|e|  Error::new(
                    libc::EIO,
                    format!("Error getting metadata object: {}", e),
                ))?;

            // Set the global
            *device = Some(BlockDeviceClient {
                runtime,
                client,
                size,
                base_name,
            });
        }
        Ok(())
    }

    fn open(_readonly: bool) -> Box<dyn Server> {
        Box::new(NbdGateway::default())
    }

    fn get_size(&self) -> Result<i64> {
        Ok(DEVICE.lock().unwrap().as_ref().unwrap().size as i64)
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        let device = DEVICE.lock().unwrap();
        let device = device.as_ref().unwrap();
        let offset = offset as usize;

        for part in list_blocks(offset, buf.len()) {
            let mut object_id = device.base_name.clone();
            write!(object_id, "_{}", part.block_num()).unwrap();
            let object_id = ObjectId(object_id);
            let data = device.runtime.block_on(device.client.read_part(
                &object_id,
                part.block_offset() as u32,
                part.size() as u32,
            ));
            let data = match data {
                Err(e) => return Err(Error::new(libc::EIO, format!("Error reading block: {}", e))),
                Ok(None) => vec![0; part.size()],
                Ok(Some(d)) => d,
            };
            buf[part.buf_start()..part.buf_end()].clone_from_slice(&data);
        }

        Ok(())
    }

    fn thread_model() -> Result<ThreadModel> where Self: Sized {
        Ok(ThreadModel::Parallel)
    }

    fn write_at(&self, buf: &[u8], offset: u64, _flags: Flags) -> Result<()> {
        let device = DEVICE.lock().unwrap();
        let device = device.as_ref().unwrap();
        let offset = offset as usize;

        for part in list_blocks(offset, buf.len()) {
            let mut object_id = device.base_name.clone();
            write!(object_id, "_{}", part.block_num()).unwrap();
            let object_id = ObjectId(object_id);
            let data = &buf[part.buf_start()..part.buf_end()];
            let res = device.runtime.block_on(device.client.write_part(
                &object_id,
                part.block_offset() as u32,
                data,
            ));
            match res {
                Err(e) => return Err(Error::new(libc::EIO, format!("Error reading block: {}", e))),
                Ok(()) => {}
            }
        }

        Ok(())
    }
}

plugin!(NbdGateway {thread_model, write_at, config, config_complete});
