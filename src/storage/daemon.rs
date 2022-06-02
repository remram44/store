use log::{error, info, warn};
use rand::{Rng, thread_rng};
use std::collections::HashMap;
use std::fs::File;
use std::io::{Error as IoError, ErrorKind, Read, Write};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::net::UdpSocket;

use crate::DeviceId;
use super::StorageBackend;
use super::file_store::FileStore;

pub struct StorageDaemon {
    /// Backend performing read and write operations.
    storage_backend: Box<dyn StorageBackend>,

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

pub async fn run_storage_daemon(
    peer_address: SocketAddr,
    peer_cert: &Path,
    peer_key: &Path,
    peer_ca_cert: &Path,
    listen_address: SocketAddr,
    storage_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize backend
    let create = if storage_dir.exists() {
        if !storage_dir.is_dir() {
            error!("Storage path exists and is not a directory");
            return Err(Box::new(IoError::new(
                ErrorKind::AlreadyExists,
                "Storage path exists and is not a directory",
            )));
        }

        // Check layout
        if storage_dir.join("store.id").is_file() {
            // It's ready to go
            info!("Using existing store");
            false
        } else {
            for entry in std::fs::read_dir(storage_dir)? {
                eprintln!("{}", entry?.file_name().to_string_lossy());
                return Err(Box::new(IoError::new(
                    ErrorKind::AlreadyExists,
                    "Storage path exists and is not an empty directory",
                )));
            }
            // It's empty
            true
        }
    } else {
        // It doesn't exist, make an empty directory
        std::fs::create_dir(storage_dir)?;
        true
    };

    let (storage_backend, device_id) = if create {
        warn!("Creating new file store");

        // Generate a random device ID
        let mut rng = thread_rng();
        let mut bytes = [0; 16];
        rng.fill(&mut bytes);
        let device_id = DeviceId(bytes);
        info!("Generated ID: {:?}", device_id);

        // Write it to "store.id"
        let mut id = File::create(storage_dir.join("store.id"))?;
        id.write_all(&device_id.0)?;

        // Open the store
        (FileStore::open(storage_dir.to_owned()), device_id)
    } else {
        // Read device ID from "store.id"
        let mut bytes = [0; 16];
        let mut id = File::open(storage_dir.join("store.id"))?;
        id.read_exact(&mut bytes)?;
        let device_id = DeviceId(bytes);
        info!("Read device ID {:?}", device_id);

        // Open the store
        (FileStore::open(storage_dir.to_owned()), device_id)
    };
    let storage_backend = Box::new(storage_backend);

    let storage_daemon = StorageDaemon {
        storage_backend,
        device_id,
        peer_address,
        listen_address,
        masters: vec![],
        storage_daemons: HashMap::new(),
    };
    let storage_daemon = Arc::new(Mutex::new(storage_daemon));

    let clients_fut = {
        info!("Listening for client connections on {}", listen_address);
        let listener: UdpSocket = UdpSocket::bind(listen_address).await?;
        serve_clients(listener, storage_daemon.clone())
    };

    clients_fut.await?;

    Ok(())
}

async fn serve_clients(listener: UdpSocket, storage_daemon: Arc<Mutex<StorageDaemon>>) -> Result<(), IoError> {
    loop {
        let mut buf = [0; 65536];
        let (len, addr) = listener.recv_from(&mut buf).await?;
        info!("Got packet from {:?}", addr);
    }
}
