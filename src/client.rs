use std::collections::HashMap;
use std::net::{TcpStream, SocketAddr};

use crate::DeviceId;
use crate::crypto::KeyPair;
use crate::storage_map;

pub struct Client {
    /// Addresses of master server(s).
    masters: Vec<SocketAddr>,

    /// Connection to master server.
    master_connection: TcpStream,

    /// The single pool we care about.
    pool: String,

    /// The storage map for the pool we care about.
    pool_storage_map: storage_map::Node,

    /// The storage daemons.
    storage_daemons: HashMap<DeviceId, StorageDaemon>,

    storage_daemon_key: KeyPair,
}

struct StorageDaemon {
    address: SocketAddr,
    client_counter: u32,
    server_counter: u32,
}
