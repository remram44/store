use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use lazy_static::lazy_static;
use log::{debug, info, warn};
use std::collections::HashMap;
use std::io::{Cursor, Error as IoError, ErrorKind, Read};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::net::UdpSocket;

use crate::{DeviceId, GroupId, ObjectId, PoolName};
use super::storage::StorageBackend;
use super::storage_map::{Node, StorageMap};

#[derive(Clone)]
struct Metrics {
    reads: prometheus::IntCounter,
    writes: prometheus::IntCounter,
    invalid_requests: prometheus::IntCounter,
}

lazy_static! {
    static ref METRICS: Metrics = {
        let m = Metrics {
            reads: prometheus::register_int_counter!("reads", "Total reads").unwrap(),
            writes: prometheus::register_int_counter!("writes", "Total writes").unwrap(),
            invalid_requests: prometheus::register_int_counter!("invalid_requests", "Total invalid requests").unwrap(),
        };
        let metrics = m.clone();
        std::thread::spawn(move || {
            let mut last_reads = 0;
            let mut last_writes = 0;
            let mut last_invalid_requests = 0;
            loop {
                let reads = metrics.reads.get();
                let writes = metrics.writes.get();
                let invalid_requests = metrics.invalid_requests.get();
                if reads != last_reads
                    || writes != last_writes
                    || invalid_requests != last_invalid_requests
                {
                    info!(
                        "last 10s: {} reads, {} writes, {} invalid requests",
                        reads - last_reads,
                        writes - last_writes,
                        invalid_requests - last_invalid_requests
                    );
                    last_reads = reads;
                    last_writes = writes;
                    last_invalid_requests = invalid_requests;
                }
                std::thread::sleep(std::time::Duration::from_millis(10000));
            }
        });
        m
    };
}

pub struct StorageDaemon {
    /// The random ID for this storage daemon.
    device_id: DeviceId,

    /// Address we listen on for other storage daemons (TCP, mTLS).
    peer_address: SocketAddr,

    /// Address we listen on for clients (UDP).
    listen_address: SocketAddr,

    /// Addresses of master server(s).
    masters: Vec<SocketAddr>,

    /// Storage pools.
    pools: HashMap<PoolName, Pool>,

    /// Addresses of all storage daemons.
    storage_daemons: HashMap<DeviceId, SocketAddr>,
}

pub enum Pool {
    /// Normal operation, a single map is in use.
    Normal(StorageMap),

    /// Preparing to transition to a new map, forward request to old location.
    TransitionPrepare { current: StorageMap, next: StorageMap },

    /// Transitioning to a new map, read from old location if necessary.
    Transition { previous: StorageMap, current: StorageMap },
}

pub async fn run_storage_daemon(
    peer_address: SocketAddr,
    peer_cert: &Path,
    peer_key: &Path,
    peer_ca_cert: &Path,
    listen_address: SocketAddr,
    storage_backend: Box<dyn StorageBackend>,
    device_id: DeviceId,
) -> Result<(), Box<dyn std::error::Error>> {
    let storage_backend: Arc<dyn StorageBackend> = storage_backend.into();

    let storage_map = StorageMap {
        generation: 1,
        groups: 128,
        replicas: 1,
        map_root: Node::Device(device_id.clone()),
    };
    let mut pools = HashMap::new();
    pools.insert(PoolName("default".to_owned()), Pool::Normal(storage_map));
    let storage_daemon = StorageDaemon {
        device_id,
        peer_address,
        listen_address,
        masters: vec![],
        pools,
        storage_daemons: HashMap::new(),
    };
    let storage_daemon = Arc::new(Mutex::new(storage_daemon));

    let clients_fut = {
        info!("Listening for client connections on {}", listen_address);
        let socket = UdpSocket::bind(listen_address).await?;
        let socket = Arc::new(socket);
        serve_clients(socket, storage_daemon.clone(), storage_backend)
    };

    clients_fut.await?;

    Ok(())
}

async fn serve_clients(socket: Arc<UdpSocket>, storage_daemon: Arc<Mutex<StorageDaemon>>, storage_backend: Arc<dyn StorageBackend>) -> Result<(), IoError> {
    loop {
        let mut buf = [0; 65536];
        let (len, addr) = socket.recv_from(&mut buf).await?;
        debug!("Got packet from {}, size {}", addr, len);
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
        Err(e) => {
            warn!("Error handling request from {}: {}", addr, e);
            METRICS.invalid_requests.inc();
        }
    }
    Ok(())
}

enum Location {
    /// We are the only primary for this object.
    Here(Vec<DeviceId>),
    /// Request should be forwarded elsewhere.
    Forward(DeviceId),
    /// We are the primary but we can request from previous location if needed.
    HereOrFallback(DeviceId, Vec<DeviceId>),
}

fn get_secondaries(map: &StorageMap, pool_name: &PoolName, group_id: &GroupId) -> Vec<DeviceId> {
    (1..map.replicas).map(|replica_id| map.group_to_device(group_id, replica_id)).collect()
}

fn get_location(storage_daemon: Arc<Mutex<StorageDaemon>>, pool_name: &PoolName, object_id: &ObjectId, device_id: &DeviceId) -> Result<Location, IoError> {
    let daemon = storage_daemon.lock().unwrap();
    let pool = match daemon.pools.get(pool_name) {
        Some(p) => p,
        None => return Err(IoError::new(ErrorKind::InvalidData, "Unknown pool")),
    };

    // Check that we are responsible for this object
    match pool {
        Pool::Normal(map) => {
            let group_id = map.object_to_group(object_id);
            let target_device = map.group_to_device(&group_id, 0);
            if &target_device == device_id {
                let secondaries = get_secondaries(map, pool_name, &group_id);
                Ok(Location::Here(secondaries))
            } else {
                Err(IoError::new(ErrorKind::Other, "Request was sent to wrong daemon"))
            }
        }
        Pool::TransitionPrepare { current, next } => {
            // We are waiting for the transition
            // During that time both locations will be getting requests from
            // clients, so keep handling them at the old location
            let current_group_id = current.object_to_group(object_id);
            let current_device = current.group_to_device(&current_group_id, 0);
            if &current_device == device_id {
                let secondaries = get_secondaries(current, pool_name, &current_group_id);
                return Ok(Location::Here(secondaries));
            }

            let next_group_id = next.object_to_group(object_id);
            let next_device = next.group_to_device(&next_group_id, 0);
            if &next_device == device_id {
                return Ok(Location::Forward(current_device));
            }

            Err(IoError::new(ErrorKind::Other, "Request was sent to wrong daemon"))
        }
        Pool::Transition { previous, current } => {
            // We are in transition
            // We have given enough time to clients to stop sending to the old
            // location, start handling requests at new location
            let current_group_id = current.object_to_group(object_id);
            let current_device = current.group_to_device(&current_group_id, 0);
            if &current_device == device_id {
                let previous_group_id = previous.object_to_group(object_id);
                let previous_device = previous.group_to_device(&previous_group_id, 0);
                let secondaries = get_secondaries(current, pool_name, &current_group_id);
                Ok(Location::HereOrFallback(previous_device, secondaries))
            } else {
                Err(IoError::new(ErrorKind::Other, "Request was sent to wrong daemon"))
            }
        }
    }
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

            debug!("read_object {:?}", object_id);
            let object = storage_backend.read_object(&pool_name, &object_id)?;
            METRICS.reads.inc();
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

            debug!("read_part {:?} {} {}", object_id, offset, len);
            let object = storage_backend.read_part(&pool_name, &object_id, offset as usize, len as usize)?;
            METRICS.reads.inc();
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

            debug!("write_object {:?} {}", object_id, data.len());
            storage_backend.write_object(&pool_name, &object_id, data)?;
            METRICS.writes.inc();
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

            debug!("write_part {:?} {} {}", object_id, offset, data.len());
            storage_backend.write_part(&pool_name, &object_id, offset, data)?;
            METRICS.writes.inc();
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

            debug!("delete_object {:?}", object_id);
            storage_backend.delete_object(&pool_name, &object_id)?;
            METRICS.writes.inc();
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
