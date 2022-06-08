use log::{error, info, warn};
use rand::{Rng, thread_rng};
use rocksdb::{DBWithThreadMode, Error as RdbError, MultiThreaded, Options};
use std::io::{Error as IoError, ErrorKind, Read, Write};
use std::fs::File;
use std::path::Path;

use crate::{DeviceId, ObjectId, PoolName};
use super::StorageBackend;

/// A storage backend using RocksDB.
pub struct RocksdbStore(DBWithThreadMode<MultiThreaded>);

/// Extension trait adding conversion of RdbError to IoError.
trait RdbToIoResultExt<T> {
    fn to_io_err(self) -> Result<T, IoError>;
}

impl<T> RdbToIoResultExt<T> for Result<T, RdbError> {
    fn to_io_err(self) -> Result<T, IoError> {
        self.map_err(|e| IoError::new(ErrorKind::Other, e.into_string()))
    }
}

impl RocksdbStore {
    pub fn open(path: &Path) -> Result<RocksdbStore, IoError> {
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DBWithThreadMode::<MultiThreaded>::open(
            &options,
            path,
        ).to_io_err()?;
        Ok(RocksdbStore(db))
    }
}

fn key(pool: &PoolName, object_id: ObjectId) -> Vec<u8> {
    let mut key = pool.0.as_bytes().to_owned();
    key.push(b'/');
    key.extend_from_slice(&object_id.0);
    key
}

impl StorageBackend for RocksdbStore {
    fn read_object(&self, pool: &PoolName, object_id: ObjectId) -> Result<Option<Vec<u8>>, IoError> {
        self.0.get(&key(pool, object_id)).to_io_err()
    }

    fn read_part(&self, pool: &PoolName, object_id: ObjectId, offset: usize, len: usize) -> Result<Option<Vec<u8>>, IoError> {
        self.read_object(pool, object_id).map(
            |r| r.map(
                |v| v[v.len().min(offset)..v.len().min(offset + len)].to_owned()
            )
        )
    }

    fn write_object(&self, pool: &PoolName, object_id: ObjectId, data: &[u8]) -> Result<(), IoError> {
        self.0.put(
            &key(pool, object_id),
            data,
        ).to_io_err()
    }

    fn write_part(&self, pool: &PoolName, object_id: ObjectId, offset: usize, data: &[u8]) -> Result<(), IoError> {
        let key = key(pool, object_id);
        match self.0.get(&key).to_io_err()? {
            Some(mut value) => {
                value.resize(value.len().max(offset + data.len()), 0);
                value[offset..offset + data.len()].clone_from_slice(data);
                self.0.put(&key, value).to_io_err()
            }
            None => {
                let mut value = Vec::with_capacity(offset + data.len());
                value.resize(offset, 0);
                value.extend_from_slice(data);
                self.0.put(&key, value).to_io_err()
            }
        }
    }

    fn delete_object(&self, pool: &PoolName, object_id: ObjectId) -> Result<(), IoError> {
        self.0.delete(&key(pool, object_id)).to_io_err()
    }
}

pub fn create_rocksdb_store(storage_dir: &Path) -> Result<(RocksdbStore, DeviceId), IoError> {
    let create = if storage_dir.exists() {
        if !storage_dir.is_dir() {
            error!("Storage path exists and is not a directory");
            return Err(IoError::new(
                ErrorKind::AlreadyExists,
                "Storage path exists and is not a directory",
            ));
        }

        // Check layout
        if storage_dir.join("store.id").is_file() {
            // It's ready to go
            info!("Using existing store");
            false
        } else {
            for _ in std::fs::read_dir(storage_dir)? {
                return Err(IoError::new(
                    ErrorKind::AlreadyExists,
                    "Storage path exists and is not an empty directory",
                ));
            }
            // It's empty
            true
        }
    } else {
        // It doesn't exist, make an empty directory
        std::fs::create_dir(storage_dir)?;
        true
    };

    if create {
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
        Ok((RocksdbStore::open(&storage_dir.to_owned())?, device_id))
    } else {
        // Read device ID from "store.id"
        let mut bytes = [0; 16];
        let mut id = File::open(storage_dir.join("store.id"))?;
        id.read_exact(&mut bytes)?;
        let device_id = DeviceId(bytes);
        info!("Read device ID {:?}", device_id);

        // Open the store
        Ok((RocksdbStore::open(&storage_dir.to_owned())?, device_id))
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;
    use std::path::Path;

    use super::RocksdbStore;

    #[test]
    fn test_rdbstore_common() {
        let path = TempDir::new("store_rocksdb_test").unwrap();
        let path: &Path = path.as_ref();
        let storage = RocksdbStore::open(path).unwrap();
        super::super::test_backend(storage);
    }
}
