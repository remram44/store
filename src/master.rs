use log::info;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Error as IoError, ErrorKind};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::{self, Certificate, PrivateKey};

use crate::DeviceId;
use crate::storage_map;

pub struct Master {
    /// Address we listen on for storage daemons (TCP, mTLS).
    peer_address: SocketAddr,

    /// Address we listen on for clients (TCP, TLS).
    listen_address: SocketAddr,

    /// The storage daemons.
    storage_daemons: HashMap<DeviceId, StorageDaemon>,

    /// The pools, with their storage maps.
    pool_storage_maps: HashMap<String, storage_map::Node>,
}

struct StorageDaemon {
    address: SocketAddr,
}

fn load_certs(path: &Path) -> Result<Vec<Certificate>, IoError> {
    rustls_pemfile::certs(&mut BufReader::new(File::open(path)?))
        .map_err(|_| IoError::new(ErrorKind::InvalidInput, "Invalid certificate file"))
        .map(|mut certs| certs.drain(..).map(Certificate).collect())
}

fn load_key(path: &Path) -> Result<PrivateKey, IoError> {
    let mut keys = rustls_pemfile::rsa_private_keys(&mut BufReader::new(File::open(path)?))
        .map_err(|_| IoError::new(ErrorKind::InvalidInput, "Invalid key file"))?;
    let mut keys = keys.drain(..).map(PrivateKey);
    let key = match keys.next() {
        Some(k) => k,
        None => return Err(IoError::new(ErrorKind::InvalidInput, "No key in file")),
    };
    if keys.next().is_some() {
        return Err(IoError::new(ErrorKind::InvalidInput, "Multiple keys in file"));
    }
    Ok(key)
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
    let master = Master {
        peer_address: peer_address.clone(),
        listen_address: listen_address.clone(),
        storage_daemons: Default::default(),
        pool_storage_maps: Default::default(),
    };
    let master = Arc::new(Mutex::new(master));

    let clients_fut = {
        info!("Listening for client connections on {}", listen_address);
        let listener: TcpListener = TcpListener::bind(&listen_address).await?;
        let certs = load_certs(listen_cert)?;
        let key = load_key(listen_key)?;
        let config = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|err| IoError::new(ErrorKind::InvalidInput, err))?;
        let acceptor = TlsAcceptor::from(Arc::new(config));
        tokio::spawn(serve_clients(listener, acceptor, master.clone()))
    };

    let peers_fut = {
        info!("Listening for peer connections on {}", peer_address);
        let listener: TcpListener = TcpListener::bind(&peer_address).await?;
        let certs = load_certs(peer_cert)?;
        let key = load_key(peer_key)?;
        let mut ca = rustls::RootCertStore::empty();
        ca.add(&load_certs(peer_ca_cert)?.remove(0))?;
        let client_verifier = rustls::server::AllowAnyAuthenticatedClient::new(ca);
        let config = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(certs, key)
            .map_err(|err| IoError::new(ErrorKind::InvalidInput, err))?;
        let acceptor = TlsAcceptor::from(Arc::new(config));
        tokio::spawn(serve_peers(listener, acceptor, master.clone()))
    };

    tokio::select! {
        _ = clients_fut => {}
        _ = peers_fut => {}
    };

    Ok(())
}

async fn serve_clients(listener: TcpListener, acceptor: TlsAcceptor, master: Arc<Mutex<Master>>) -> Result<(), IoError> {
    loop {
        let (stream, peer_addr) = listener.accept().await?;
        info!("Client connected from {}", peer_addr);
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            let mut stream = acceptor.accept(stream).await?;
            stream.write_all(b"Hello").await?;
            stream.shutdown().await?;
            Ok(()) as Result<(), IoError>
        });
    }
}

async fn serve_peers(listener: TcpListener, acceptor: TlsAcceptor, master: Arc<Mutex<Master>>) -> Result<(), IoError> {
    loop {
        let (stream, peer_addr) = listener.accept().await?;
        info!("Peer connected from {}", peer_addr);
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            let mut stream = acceptor.accept(stream).await?;
            stream.write_all(b"Hello").await?;
            stream.shutdown().await?;
            Ok(()) as Result<(), IoError>
        });
    }
}
