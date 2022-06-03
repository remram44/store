use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use log::{error, info, warn};
use rand::{Rng, thread_rng};
use std::collections::HashMap;
use std::fs::File;
use std::io::{Cursor, Error as IoError, ErrorKind, Read, Write};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::net::UdpSocket;

use crate::{DeviceId, ObjectId, PoolName};
use super::StorageBackend;
use super::file_store::FileStore;

pub struct StorageDaemon {
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
            for _ in std::fs::read_dir(storage_dir)? {
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
    let storage_backend = Arc::new(storage_backend);

    let storage_daemon = StorageDaemon {
        device_id,
        peer_address,
        listen_address,
        masters: vec![],
        storage_daemons: HashMap::new(),
    };
    let storage_daemon = Arc::new(Mutex::new(storage_daemon));

    let clients_fut = {
        info!("Listening for client connections on {}", listen_address);
        let socket = UdpSocket::bind(listen_address).await?;
        let socket = Arc::new(socket);
        serve_clients(socket, storage_daemon.clone(), storage_backend.clone())
    };

    clients_fut.await?;

    Ok(())
}

async fn serve_clients(socket: Arc<UdpSocket>, storage_daemon: Arc<Mutex<StorageDaemon>>, storage_backend: Arc<dyn StorageBackend>) -> Result<(), IoError> {
    loop {
        let mut buf = [0; 65536];
        let (len, addr) = socket.recv_from(&mut buf).await?;
        info!("Got packet from {}, size {}", addr, len);
        let msg = buf[0..len].to_owned();

        tokio::spawn(handle_client_request(
            socket.clone(),
            storage_daemon.clone(),
            storage_backend.clone(),
            addr,
            msg,
        ));
    }
}

async fn handle_client_request(socket: Arc<UdpSocket>, storage_daemon: Arc<Mutex<StorageDaemon>>, storage_backend: Arc<dyn StorageBackend>, addr: SocketAddr, msg: Vec<u8>) -> Result<(), IoError> {
    match handle_client_request_inner(socket, storage_daemon, storage_backend, addr, msg).await {
        Ok(()) => {}
        Err(e) => warn!("Error handling request from {}: {}", addr, e),
    }
    Ok(())
}

async fn handle_client_request_inner(socket: Arc<UdpSocket>, storage_daemon: Arc<Mutex<StorageDaemon>>, storage_backend: Arc<dyn StorageBackend>, addr: SocketAddr, msg: Vec<u8>) -> Result<(), IoError> {
    let mut reader = Cursor::new(&msg);
    let msg_ctr = reader.read_u32::<BigEndian>()?;

    let pool_name = {
        let name_len = reader.read_u32::<BigEndian>()? as usize;
        let mut pool_name = vec![0; name_len];
        reader.read_exact(&mut pool_name)?;
        let pool_name = String::from_utf8(pool_name)
            .map_err(|_| IoError::new(ErrorKind::InvalidData, "Invalid pool name"))?;
        PoolName(pool_name)
    };

    let command = reader.read_u8()?;
    match command {
        0x01 => { // read_object
            let object_id = {
                let object_id_len = reader.read_u32::<BigEndian>()? as usize;
                let mut object_id = vec![0; object_id_len];
                reader.read_exact(&mut object_id)?;
                ObjectId(object_id)
            };

            info!("read_object {:?}", object_id);
            let object = storage_backend.read_object(&pool_name, object_id)?;
            let mut response = Vec::new();
            response.write_u32::<BigEndian>(msg_ctr).unwrap();
            match object {
                Some(data) => {
                    response.write_u8(1).unwrap();
                    response.extend_from_slice(&data);
                }
                None => response.write_u8(0).unwrap(),
            }
            socket.send_to(&response, addr).await?;
        }
        0x02 => { // read_part
            let object_id = {
                let object_id_len = reader.read_u32::<BigEndian>()? as usize;
                let mut object_id = vec![0; object_id_len];
                reader.read_exact(&mut object_id)?;
                ObjectId(object_id)
            };

            let offset = reader.read_u32::<BigEndian>()?;
            let len = reader.read_u32::<BigEndian>()?;

            info!("read_part {:?} {} {}", object_id, offset, len);
            let object = storage_backend.read_part(&pool_name, object_id, offset as usize, len as usize)?;
            let mut response = Vec::new();
            response.write_u32::<BigEndian>(msg_ctr).unwrap();
            match object {
                Some(data) => {
                    response.write_u8(1).unwrap();
                    response.extend_from_slice(&data);
                }
                None => response.write_u8(0).unwrap(),
            }
            socket.send_to(&response, addr).await?;
        }
        0x03 => { // write_object
            let object_id = {
                let object_id_len = reader.read_u32::<BigEndian>()? as usize;
                let mut object_id = vec![0; object_id_len];
                reader.read_exact(&mut object_id)?;
                ObjectId(object_id)
            };

            let data = &msg[reader.position() as usize..];

            info!("write_object {:?} {}", object_id, data.len());
            storage_backend.write_object(&pool_name, object_id, data)?;
            let mut response = Vec::with_capacity(4);
            response.write_u32::<BigEndian>(msg_ctr).unwrap();
            socket.send_to(&response, addr).await?;
        }
        0x04 => { // write_part
            let object_id = {
                let object_id_len = reader.read_u32::<BigEndian>()? as usize;
                let mut object_id = vec![0; object_id_len];
                reader.read_exact(&mut object_id)?;
                ObjectId(object_id)
            };

            let offset = reader.read_u32::<BigEndian>()? as usize;
            let data = &msg[reader.position() as usize..];

            info!("write_part {:?} {} {}", object_id, offset, data.len());
            storage_backend.write_part(&pool_name, object_id, offset, data)?;
            let mut response = Vec::with_capacity(4);
            response.write_u32::<BigEndian>(msg_ctr).unwrap();
            socket.send_to(&response, addr).await?;
        }
        0x05 => { // delete_object
            let object_id = {
                let object_id_len = reader.read_u32::<BigEndian>()? as usize;
                let mut object_id = vec![0; object_id_len];
                reader.read_exact(&mut object_id)?;
                ObjectId(object_id)
            };

            info!("delete_object {:?}", object_id);
            storage_backend.delete_object(&pool_name, object_id)?;
            let mut response = Vec::with_capacity(4);
            response.write_u32::<BigEndian>(msg_ctr).unwrap();
            socket.send_to(&response, addr).await?;
        }
        _ => return Err(IoError::new(
            ErrorKind::InvalidData,
            format!("Unknown command 0x{:02x} from client", command),
        )),
    }

    Ok(())
}
