use std::fs::{File, OpenOptions, remove_file};
use std::io::{Error as IoError, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::{ObjectId, PoolName};
use super::StorageBackend;

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
    let mut result = Vec::new();
    result.extend_from_slice(pool.0.as_bytes());
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
        let mut result = vec![0; len];
        file.read_exact(&mut result)?;
        Ok(Some(result))
    }

    fn write_object(&self, pool: &PoolName, object_id: ObjectId, data: &[u8]) -> Result<(), IoError> {
        let pool_path = self.path.join(&pool.0);
        if !pool_path.exists() {
            std::fs::create_dir(pool_path)?;
        }
        let enc_id = encode_object_id(pool, object_id);
        let path = self.path.join(enc_id);
        let mut file = OpenOptions::new().create(true).write(true).truncate(true).open(path)?;
        file.write_all(data)
    }

    fn write_part(&self, pool: &PoolName, object_id: ObjectId, offset: usize, data: &[u8]) -> Result<(), IoError> {
        let pool_path = self.path.join(&pool.0);
        if !pool_path.exists() {
            std::fs::create_dir(pool_path)?;
        }
        let enc_id = encode_object_id(pool, object_id);
        let path = self.path.join(enc_id);
        let mut file = OpenOptions::new().create(true).write(true).truncate(false).open(path)?;
        let size = file.seek(SeekFrom::End(0))?;
        file.seek(SeekFrom::Start(offset as u64))?;
        file.write_all(data)?;
        file.set_len(size.max((offset + data.len()) as u64))
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

#[cfg(test)]
mod tests {
    use crate::ObjectId;
    use super::encode_object_id;

    #[test]
    fn test_encode() {
        assert_eq!(
            encode_object_id(ObjectId((b"hello\0world!" as &[u8]).to_owned())),
            "68656c6c6f00776f726c6421",
        );
    }
}
