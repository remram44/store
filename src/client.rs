use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use lazy_static::lazy_static;
use log::info;
use std::collections::HashMap;
use std::net::{TcpStream, SocketAddr};
use std::io::{Cursor, Error as IoError, ErrorKind, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::oneshot::{Sender, channel};

use crate::{DeviceId, ObjectId, PoolName};
use crate::crypto::KeyPair;
use crate::storage_map;

#[derive(Clone)]
struct Metrics {
    reads: prometheus::IntCounter,
    writes: prometheus::IntCounter,
    resends: prometheus::IntCounter,
    in_flight: prometheus::IntGauge,
}

lazy_static! {
    static ref METRICS: Metrics = {
        let m = Metrics {
            reads: prometheus::register_int_counter!("reads", "Total reads").unwrap(),
            writes: prometheus::register_int_counter!("writes", "Total writes").unwrap(),
            resends: prometheus::register_int_counter!("resends", "Total resent packets").unwrap(),
            in_flight: prometheus::register_int_gauge!("in_flight", "Requests currently in flight").unwrap(),
        };
        let metrics = m.clone();
        std::thread::spawn(move || {
            let mut last_reads = 0;
            let mut last_writes = 0;
            let mut last_resends = 0;
            loop {
                let reads = metrics.reads.get();
                let writes = metrics.writes.get();
                let resends = metrics.resends.get();
                if reads != last_reads || writes != last_writes || resends != last_resends {
                    info!(
                        "last 10s: {} reads, {} writes, {} resent packets",
                        reads - last_reads,
                        writes - last_writes,
                        resends - last_resends
                    );
                    last_reads = reads;
                    last_writes = writes;
                    last_resends = resends;
                }
                std::thread::sleep(std::time::Duration::from_millis(10000));
            }
        });
        m
    };
}

pub struct ClientInner {
    /// Addresses of master server(s).
    masters: Vec<SocketAddr>,

    /// Connection to master server.
    master_connection: Option<TcpStream>,

    /// The single pool we care about.
    pool: PoolName,

    /// The storage map for the pool we care about.
    pool_config: storage_map::StorageConfiguration,

    /// The storage daemons.
    storage_daemons: HashMap<DeviceId, StorageDaemon>,

    storage_daemon_key: KeyPair,

    /// Map of channels to get responses from the reading task.
    response_channels: HashMap<(SocketAddr, u32), (Instant, Sender<Vec<u8>>)>,
}

struct StorageDaemon {
    address: SocketAddr,
    client_counter: u32,
    server_counter: u32,
}

const TIMEOUT: Duration = Duration::from_millis(200);

#[derive(Clone)]
pub struct Client {
    client: Arc<Mutex<ClientInner>>,
    udp_socket: Arc<UdpSocket>,
    _receive_task_handle: Arc<CancelTask>,
}

struct CancelTask(tokio::task::JoinHandle<Result<(), IoError>>);

impl Drop for CancelTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Client {
    pub async fn read_object(&self, object_id: &ObjectId) -> Result<Option<Vec<u8>>, IoError> {
        // Do the request
        METRICS.reads.inc();
        let response = self.do_request(object_id, |req| {
            req.write_u8(0x01).unwrap(); // read_object
            req.write_u32::<BigEndian>(object_id.0.len() as u32).unwrap();
            req.write_all(&object_id.0).unwrap();
        }).await?;

        // Read the response
        if response.len() < 5 {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                "Invalid reply from storage daemon",
            ));
        }
        match response[4] {
            1 => Ok(Some(response[5..].to_owned())),
            0 => Ok(None),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                "Invalid reply from storage daemon",
            )),
        }
    }

    pub async fn read_part(&self, object_id: &ObjectId, offset: u32, len: u32) -> Result<Option<Vec<u8>>, IoError> {
        // Do the request
        METRICS.reads.inc();
        let response = self.do_request(object_id, |req| {
            req.write_u8(0x02).unwrap(); // read_part
            req.write_u32::<BigEndian>(object_id.0.len() as u32).unwrap();
            req.write_all(&object_id.0).unwrap();
            req.write_u32::<BigEndian>(offset).unwrap();
            req.write_u32::<BigEndian>(len).unwrap();
        }).await?;

        // Read the response
        if response.len() < 5 {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                "Invalid reply from storage daemon",
            ));
        }
        match response[4] {
            1 => Ok(Some(response[5..].to_owned())),
            0 => Ok(None),
            _ => Err(IoError::new(ErrorKind::InvalidData, "Invalid reply from storage daemon")),
        }
    }

    pub async fn write_object(&self, object_id: &ObjectId, data: &[u8]) -> Result<(), IoError> {
        // Do the request
        METRICS.writes.inc();
        let response = self.do_request(object_id, |req| {
            req.write_u8(0x03).unwrap(); // write_object
            req.write_u32::<BigEndian>(object_id.0.len() as u32).unwrap();
            req.write_all(&object_id.0).unwrap();
            req.write_all(data).unwrap();
        }).await?;

        // Read the response
        if response.len() != 4 {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                "Invalid reply from storage daemon",
            ));
        }

        Ok(())
    }

    pub async fn write_part(&self, object_id: &ObjectId, offset: u32, data: &[u8]) -> Result<(), IoError> {
        // Do the request
        METRICS.writes.inc();
        let response = self.do_request(object_id, |req| {
            req.write_u8(0x04).unwrap(); // write_part
            req.write_u32::<BigEndian>(object_id.0.len() as u32).unwrap();
            req.write_all(&object_id.0).unwrap();
            req.write_u32::<BigEndian>(offset).unwrap();
            req.write_all(data).unwrap();
        }).await?;

        // Read the response
        if response.len() != 4 {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                "Invalid reply from storage daemon",
            ));
        }

        Ok(())
    }

    pub async fn delete_object(&self, object_id: &ObjectId) -> Result<(), IoError> {
        // Do the request
        METRICS.writes.inc();
        let response = self.do_request(object_id, |req| {
            req.write_u8(0x05).unwrap(); // delete_object
            req.write_u32::<BigEndian>(object_id.0.len() as u32).unwrap();
            req.write_all(&object_id.0).unwrap();
        }).await?;

        // Read the response
        if response.len() != 4 {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                "Invalid reply from storage daemon",
            ));
        }

        Ok(())
    }

    async fn do_request<F: FnOnce(&mut Vec<u8>)>(&self, object_id: &ObjectId, write_request: F) -> Result<Vec<u8>, IoError> {
        let mut client = self.client.lock().unwrap();
        let group_id = client.pool_config.object_to_group(object_id);
        let device_id = client.pool_config.group_to_device(&group_id, 0);
        let daemon = client.storage_daemons.get_mut(&device_id).unwrap();
        let counter = daemon.client_counter;
        daemon.client_counter += 1;
        let address = daemon.address.clone();

        // Assemble the request
        let mut request = Vec::new();
        request.write_u32::<BigEndian>(counter).unwrap();
        request.write_u32::<BigEndian>(client.pool.0.len() as u32).unwrap();
        request.write_all(client.pool.0.as_bytes()).unwrap();
        write_request(&mut request);

        // Register our counter to get response
        let (send, mut recv) = channel();
        client.response_channels.insert((address, counter), (Instant::now(), send));

        // Unlock the mutex during network operations
        drop(client);

        info!("Sending request {}, size {}", counter, request.len());
        METRICS.in_flight.inc();
        loop {
            // Send the request
            self.udp_socket.send_to(&request, address).await?;

            // Wait for the response or timeout
            tokio::select! {
                response = &mut recv => {
                    METRICS.in_flight.dec();
                    return Ok(response.unwrap());
                }
                _ = tokio::time::sleep(TIMEOUT) => {}
            }
            info!("Timeout, resending request {}", counter);
            METRICS.resends.inc();
        }
    }
}

pub async fn create_client(storage_daemon_address: SocketAddr, pool: PoolName) -> Result<Client, Box<dyn std::error::Error>> {
    let storage_daemon_key = KeyPair {
        mac_key: *b"0123456789abcdef",
        encrypt_key: *b"0123456789abcdef",
    };

    let device_id = DeviceId([0; 16]);
    let pool_config = storage_map::StorageConfiguration {
        groups: 128,
        map_root: storage_map::Node::Device(device_id.clone()),
    };
    let mut storage_daemons = HashMap::new();
    storage_daemons.insert(
        device_id,
        StorageDaemon {
            address: storage_daemon_address,
            client_counter: 0,
            server_counter: 0,
        },
    );

    let client_inner = ClientInner {
        masters: vec![],
        master_connection: None,
        pool,
        pool_config,
        storage_daemons,
        storage_daemon_key,
        response_channels: HashMap::new(),
    };
    let client_inner = Arc::new(Mutex::new(client_inner));

    let udp_socket = UdpSocket::bind("0.0.0.0:0").await?;
    let udp_socket = Arc::new(udp_socket);

    // Start the receiving task
    let receive_task_handle = tokio::spawn(receive_task(client_inner.clone(), udp_socket.clone()));

    // Wrap the receiving task handle in a structure that will drop it when no
    // client remains
    let receive_task_handle = Arc::new(CancelTask(receive_task_handle));

    let client = Client {
        client: client_inner,
        udp_socket,
        _receive_task_handle: receive_task_handle,
    };

    Ok(client)
}

async fn receive_task(client: Arc<Mutex<ClientInner>>, udp_socket: Arc<UdpSocket>) -> Result<(), IoError> {
    let udp_socket: &UdpSocket = &udp_socket;
    let mut buf = [0; 65536];
    loop {
        let (len, addr) = udp_socket.recv_from(&mut buf).await?;
        info!("Got packet from {}, size {}", addr, len);
        let msg = &buf[0..len];
        if msg.len() < 4 {
            continue;
        }
        let counter = Cursor::new(msg).read_u32::<BigEndian>().unwrap();

        // Get the channel
        let mut client = client.lock().unwrap();
        if let Some((_, channel)) = client.response_channels.remove(&(addr, counter)) {
            info!("Handling reply, counter={}", counter);
            channel.send(msg.to_owned()).unwrap();
        }
    }
}
