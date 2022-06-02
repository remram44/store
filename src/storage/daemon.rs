use std::collections::HashMap;
use std::net::SocketAddr;

use crate::DeviceId;
use super::StorageBackend;

pub struct StorageDaemon {
    /// Backend performing read and write operations.
    storage: Box<dyn StorageBackend>,

    /// The random ID for this storage daemon.
    device_id: DeviceId,

    /// Address we listen on for other storage daemons (TCP, mTLS).
    peer_address: SocketAddr,

    /// Address we listen on for clients (UDP).
    listen_address: SocketAddr,

    /// Addresses of master server(s).
    masters: Vec<SocketAddr>,

    /// Active storage daemon connections.
    storage_daemons: HashMap<DeviceId, StorageDaemonPeer>,
}

struct StorageDaemonPeer {
}
