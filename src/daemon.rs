use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use lazy_static::lazy_static;
use log::{debug, info, warn};
use std::collections::HashMap;
use std::io::{Cursor, Error as IoError, ErrorKind, Read};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::oneshot::{Sender, channel};

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

const TIMEOUT: Duration = Duration::from_millis(5000);

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
    storage_daemons: HashMap<DeviceId, Arc<Mutex<PeerDaemon>>>,
}

pub struct PeerDaemon {
    address: SocketAddr,
    counter: u32,
    response_channels: HashMap<u32, (Instant, Sender<Vec<u8>>)>,
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
    /// We are the primary, but we can request from previous location if set.
    HereOrFallback(Option<(DeviceId, Arc<Mutex<PeerDaemon>>)>, Vec<(DeviceId, Arc<Mutex<PeerDaemon>>)>),
    /// Request should be forwarded elsewhere.
    Forward(Arc<Mutex<PeerDaemon>>),
}

fn get_secondaries(map: &StorageMap, storage_daemons: &HashMap<DeviceId, Arc<Mutex<PeerDaemon>>>, group_id: &GroupId) -> Result<Vec<(DeviceId, Arc<Mutex<PeerDaemon>>)>, IoError> {
    let mut secondaries = Vec::with_capacity(map.replicas as usize - 1);
    for replica_id in 1..map.replicas {
        let device_id = map.group_to_device(group_id, replica_id);
        let peer = storage_daemons
            .get(&device_id)
            .ok_or(IoError::new(ErrorKind::NotFound, "No address for device"))?
            .clone();
        secondaries.push((device_id, peer));
    }
    Ok(secondaries)
}

fn get_location(storage_daemon: Arc<Mutex<StorageDaemon>>, pool_name: &PoolName, object_id: &ObjectId) -> Result<Location, IoError> {
    let daemon = storage_daemon.lock().unwrap();
    let device_id = &daemon.device_id;
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
                let secondaries = get_secondaries(map, &daemon.storage_daemons, &group_id)?;
                Ok(Location::HereOrFallback(None, secondaries))
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
                let secondaries = get_secondaries(current, &daemon.storage_daemons, &current_group_id)?;
                return Ok(Location::HereOrFallback(None, secondaries));
            }

            let next_group_id = next.object_to_group(object_id);
            let next_device = next.group_to_device(&next_group_id, 0);
            if &next_device == device_id {
                let current_addr = daemon.storage_daemons
                    .get(&current_device)
                    .ok_or(IoError::new(ErrorKind::NotFound, "No address for device"))?
                    .clone();
                return Ok(Location::Forward(current_addr));
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
                let previous_peer = daemon.storage_daemons
                    .get(&previous_device)
                    .ok_or(IoError::new(ErrorKind::NotFound, "No address for device"))?
                    .clone();
                let secondaries = get_secondaries(current, &daemon.storage_daemons, &current_group_id)?;
                Ok(Location::HereOrFallback(Some((previous_device, previous_peer)), secondaries))
            } else {
                Err(IoError::new(ErrorKind::Other, "Request was sent to wrong daemon"))
            }
        }
    }
}

async fn handle_client_request_inner(socket: Arc<UdpSocket>, storage_daemon: Arc<Mutex<StorageDaemon>>, storage_backend: Arc<dyn StorageBackend>, client_addr: SocketAddr, msg: Vec<u8>) -> Result<(), IoError> {
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

            match get_location(storage_daemon, &pool_name, &object_id)? {
                Location::HereOrFallback(fallback, _secondaries) => {
                    let object = storage_backend.read_object(&pool_name, &object_id)?;
                    METRICS.reads.inc();
                    let mut response = Vec::new();
                    response.write_u32::<BigEndian>(msg_ctr).unwrap();
                    match object {
                        Some(data) => {
                            response.write_u8(1).unwrap();
                            response.extend_from_slice(&data);
                        }
                        // TODO: fallback
                        None => response.write_u8(0).unwrap(),
                    }
                    socket.send_to(&response, client_addr).await?;
                }
                Location::Forward(peer) => {
                    forward_request(&socket, msg_ctr, peer, &msg[4..], client_addr).await?;
                }
            }
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

            match get_location(storage_daemon, &pool_name, &object_id)? {
                Location::HereOrFallback(fallback, _secondaries) => {
                    let object = storage_backend.read_part(&pool_name, &object_id, offset as usize, len as usize)?;
                    METRICS.reads.inc();
                    let mut response = Vec::new();
                    response.write_u32::<BigEndian>(msg_ctr).unwrap();
                    match object {
                        Some(data) => {
                            response.write_u8(1).unwrap();
                            response.extend_from_slice(&data);
                        }
                        // TODO: fallback
                        None => response.write_u8(0).unwrap(),
                    }
                    socket.send_to(&response, client_addr).await?;
                }
                Location::Forward(peer) => {
                    forward_request(&socket, msg_ctr, peer, &msg[4..], client_addr).await?;
                }
            }
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

            match get_location(storage_daemon, &pool_name, &object_id)? {
                Location::HereOrFallback(_fallback, _secondaries) => {
                    storage_backend.write_object(&pool_name, &object_id, data)?;
                    METRICS.writes.inc();
                    // TODO: replicate to secondaries
                    let mut response = Vec::new();
                    response.write_u32::<BigEndian>(msg_ctr).unwrap();
                    socket.send_to(&response, client_addr).await?;
                }
                Location::Forward(peer) => {
                    forward_request(&socket, msg_ctr, peer, &msg[4..], client_addr).await?;
                }
            }
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

            match get_location(storage_daemon, &pool_name, &object_id)? {
                Location::HereOrFallback(fallback, secondaries) => {
                    // TODO: fallback
                    storage_backend.write_part(&pool_name, &object_id, offset, data)?;
                    METRICS.writes.inc();
                    // TODO: replicate to secondaries
                    let mut response = Vec::new();
                    response.write_u32::<BigEndian>(msg_ctr).unwrap();
                    socket.send_to(&response, client_addr).await?;
                }
                Location::Forward(peer) => {
                    forward_request(&socket, msg_ctr, peer, &msg[4..], client_addr).await?;
                }
            }
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
            let mut response = Vec::new();
            response.write_u32::<BigEndian>(msg_ctr).unwrap();
            socket.send_to(&response, client_addr).await?;
        }
        _ => return Err(IoError::new(
            ErrorKind::InvalidData,
            format!("Unknown command 0x{:02x} from client", command),
        )),
    }

    Ok(())
}

async fn forward_request(socket: &UdpSocket, client_ctr: u32, peer: Arc<Mutex<PeerDaemon>>, request: &[u8], client_addr: SocketAddr) -> Result<(), IoError> {
    let (address, counter, new_request, mut recv) = {
        let mut peer_locked = peer.lock().unwrap();
        let address = peer_locked.address.clone();

        // Get a request ID to read the response
        let counter = peer_locked.counter;
        peer_locked.counter += 1;

        // Assemble the request
        let mut new_request = Vec::with_capacity(4 + request.len());
        new_request.write_u32::<BigEndian>(counter).unwrap();
        new_request.extend_from_slice(request);

        // Register our counter to get the response
        let (send, recv) = channel();
        peer_locked.response_channels.insert(counter, (Instant::now(), send));

        // Unlock the mutex during network operations

        debug!("Sending forwarded request {}, size {}", counter, new_request.len());
        (address, counter, new_request, recv)
    };

    // Send the request
    socket.send_to(&new_request, address).await?;

    // Wait for the response
    let mut response = tokio::select! {
        response = &mut recv => response.unwrap(),
        _ = tokio::time::sleep(TIMEOUT) => {
            debug!("Timeout forwarding request {}", counter);
            return Err(IoError::new(ErrorKind::TimedOut, "Timeout waiting for response to forwarded request"));
        }
    };

    // Send response to client
    Cursor::new(&mut response[0..4]).write_u32::<BigEndian>(client_ctr).unwrap();
    debug!("Sending forwarded response to client, size {}", response.len());
    socket.send_to(&response, client_addr).await?;

    Ok(())
}
