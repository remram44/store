use lazy_static::lazy_static;
use std::net::SocketAddr;
use std::sync::Mutex;

use nbdkit::*;
use store::PoolName;

// The RAM disk.
lazy_static! {
    static ref DISK: Mutex<Vec<u8>> = Mutex::new (vec![0; 100 * 1024 * 1024]);
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
}

lazy_static! {
    static ref CONFIG: Mutex<NbdGatewayConfig> = Mutex::new(NbdGatewayConfig::default());
}

const CONFIG_HELP: &'static str = "\
Configuration options (pass KEY=VALUE on command line):
    storage_daemon_address: address and UDP port of the storage daemon
    pool: name of the pool
    image: base name of the block device objects in the pool
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
        } else {
            return Err(Error::new(libc::EINVAL, format!("Invalid configuration option {}", key)));
        }
        Ok(())
    }

    fn config_complete() -> Result<()> {
        let config = CONFIG.lock().unwrap();
        if config.storage_daemon_address.is_none() {
            Err(Error::new(libc::EINVAL, "Missing option storage_daemon_address"))
        } else if config.pool.is_none() {
            Err(Error::new(libc::EINVAL, "Missing option pool"))
        } else if config.image.is_none() {
            Err(Error::new(libc::EINVAL, "Missing option image"))
        } else {
            Ok(())
        }
    }

    fn open(_readonly: bool) -> Box<dyn Server> {
        Box::new(NbdGateway::default())
    }

    fn get_size(&self) -> Result<i64> {
        Ok(DISK.lock().unwrap().len() as i64)
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        let disk = DISK.lock().unwrap();
        let ofs = offset as usize;
        let end = ofs + buf.len();
        buf.copy_from_slice(&disk[ofs..end]);
        Ok(())
    }

    fn thread_model() -> Result<ThreadModel> where Self: Sized {
        Ok(ThreadModel::Parallel)
    }

    fn write_at(&self, buf: &[u8], offset: u64, _flags: Flags) -> Result<()> {
        let mut disk = DISK.lock().unwrap();
        let ofs = offset as usize;
        let end = ofs + buf.len();
        disk[ofs..end].copy_from_slice(buf);
        Ok(())
    }
}

plugin!(NbdGateway {thread_model, write_at, config, config_complete});
