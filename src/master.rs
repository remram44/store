use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;

use crate::DeviceId;
use crate::storage_map;

pub struct Master {
    /// Address we listen on for storage daemons (TCP, mTLS)
    peer_address: SocketAddr,

    /// Address we listen on for clients (TCP, TLS)
    listen_address: SocketAddr,

    /// The storage daemons
    storage_daemons: HashMap<DeviceId, StorageDaemon>,

    /// The pools, with their storage maps
    pool_storage_maps: HashMap<String, storage_map::Node>,
}

struct StorageDaemon {
    address: SocketAddr,
}

pub async fn run_master(
    peer_address: SocketAddr,
    peer_cert: &Path,
    peer_key: &Path,
    peer_ca_cert: &Path,
    listen_address: SocketAddr,
    listen_cert: &Path,
    listen_key: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
}
