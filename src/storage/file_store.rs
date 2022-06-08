use log::{error, info, warn};
use rand::{Rng, thread_rng};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions, remove_file};
use std::io::{Error as IoError, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::{DeviceId, ObjectId, PoolName};
use super::StorageBackend;

/// A storage backend storing each object in a separate file.
pub struct FileStore {
    path: PathBuf,
}

impl FileStore {
    pub fn open(path: PathBuf) -> FileStore {
        FileStore {
            path,
        }
    }
}

fn encode_object_id(pool: &PoolName, object_id: ObjectId) -> String {
    // <pool>/
    let mut result = Vec::new();
    result.extend_from_slice(pool.0.as_bytes());
    result.push(b'/');

    let mut h = Sha256::new();
    h.update(&object_id.0);
    let h = h.finalize();

    // hash[0..2]/object_id
    for b in &h[0..2] {
        write!(result, "{:02x}", b).unwrap();
    }
    result.push(b'/');
    for b in &object_id.0 {
        write!(result, "{:02x}", b).unwrap();
    }
    String::from_utf8(result).unwrap()
}

impl StorageBackend for FileStore {
    fn read_object(&self, pool: &PoolName, object_id: ObjectId) -> Result<Option<Vec<u8>>, IoError> {
        let enc_id = encode_object_id(pool, object_id);
        let path = self.path.join(enc_id);
        let mut file = match File::open(path) {
            Ok(f) => Ok(f),
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => Err(e),
        }?;
        let mut result = Vec::new();
        file.read_to_end(&mut result)?;
        Ok(Some(result))
    }

    fn read_part(&self, pool: &PoolName, object_id: ObjectId, offset: usize, len: usize) -> Result<Option<Vec<u8>>, IoError> {
        let enc_id = encode_object_id(pool, object_id);
        let path = self.path.join(enc_id);
        let mut file = match File::open(path) {
            Ok(f) => Ok(f),
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => Err(e),
        }?;
        file.seek(SeekFrom::Start(offset as u64))?;
        const BUF_SIZE: usize = 4096;
        let mut result = vec![0; BUF_SIZE.min(len)];
        let mut read_bytes = 0;
        while read_bytes < len {
            let s = len.min(result.len());
            let n = file.read(&mut result[read_bytes..s])?;
            if n == 0 {
                break;
            }
            read_bytes += n;
            result.resize(len.min(read_bytes + BUF_SIZE), 0);
        }
        result.resize(read_bytes, 0);
        Ok(Some(result))
    }

    fn write_object(&self, pool: &PoolName, object_id: ObjectId, data: &[u8]) -> Result<(), IoError> {
        let enc_id = encode_object_id(pool, object_id);
        let path = self.path.join(enc_id);
        std::fs::create_dir_all(path.parent().unwrap())?;
        let mut file = OpenOptions::new().create(true).write(true).truncate(true).open(path)?;
        file.write_all(data)
    }

    fn write_part(&self, pool: &PoolName, object_id: ObjectId, offset: usize, data: &[u8]) -> Result<(), IoError> {
        let enc_id = encode_object_id(pool, object_id);
        let path = self.path.join(enc_id);
        std::fs::create_dir_all(path.parent().unwrap())?;
        let mut file = OpenOptions::new().create(true).write(true).truncate(false).open(path)?;
        file.seek(SeekFrom::Start(offset as u64))?;
        file.write_all(data)
    }

    fn delete_object(&self, pool: &PoolName, object_id: ObjectId) -> Result<(), IoError> {
        let enc_id = encode_object_id(pool, object_id);
        let path = self.path.join(enc_id);
        match remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

pub fn create_file_store(storage_dir: &Path) -> Result<(FileStore, DeviceId), IoError> {
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
        Ok((FileStore::open(storage_dir.to_owned()), device_id))
    } else {
        // Read device ID from "store.id"
        let mut bytes = [0; 16];
        let mut id = File::open(storage_dir.join("store.id"))?;
        id.read_exact(&mut bytes)?;
        let device_id = DeviceId(bytes);
        info!("Read device ID {:?}", device_id);

        // Open the store
        Ok((FileStore::open(storage_dir.to_owned()), device_id))
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;
    use std::path::Path;

    use crate::{ObjectId, PoolName};
    use super::{FileStore, encode_object_id};

    #[test]
    fn test_encode() {
        assert_eq!(
            encode_object_id(&PoolName("testpool".to_owned()), ObjectId((b"hello\0world!" as &[u8]).to_owned())),
            "testpool/6d74/68656c6c6f00776f726c6421",
        );
    }

    #[test]
    fn test_filestore_common() {
        let path = TempDir::new("store_filestore_test").unwrap();
        let path: &Path = path.as_ref();
        let storage = FileStore::open(path.to_owned());
        super::super::test_backend(storage);
    }
}
