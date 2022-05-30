use std::fs::{File, OpenOptions};
use std::io::{Error as IoError, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::{ObjectId};
use super::StorageBackend;

pub struct FileStore {
    path: PathBuf,
}

impl FileStore {
    pub fn new(path: PathBuf) -> FileStore {
        FileStore {
            path,
        }
    }
}

fn encode_object_id(object_id: ObjectId) -> String {
    todo!()
}

impl StorageBackend for FileStore {
    fn read_object(&mut self, object_id: ObjectId) -> Result<Vec<u8>, IoError> {
        let enc_id = encode_object_id(object_id);
        let path = self.path.join(enc_id);
        let mut file = File::open(path)?;
        let mut result = Vec::new();
        file.read_to_end(&mut result)?;
        Ok(result)
    }

    fn read_part(&mut self, object_id: ObjectId, offset: usize, len: usize) -> Result<Vec<u8>, IoError> {
        let enc_id = encode_object_id(object_id);
        let path = self.path.join(enc_id);
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(offset as u64))?;
        let mut result = vec![0; len];
        file.read_exact(&mut result)?;
        Ok(result)
    }

    fn write_object(&mut self, object_id: ObjectId, data: &[u8]) -> Result<(), IoError> {
        let enc_id = encode_object_id(object_id);
        let path = self.path.join(enc_id);
        let mut file = OpenOptions::new().create(true).write(true).truncate(true).open(path)?;
        file.write_all(data)
    }

    fn write_part(&mut self, object_id: ObjectId, offset: usize, data: &[u8]) -> Result<(), IoError> {
        let enc_id = encode_object_id(object_id);
        let path = self.path.join(enc_id);
        let mut file = OpenOptions::new().create(true).write(true).truncate(false).open(path)?;
        file.seek(SeekFrom::Start(offset as u64))?;
        file.write_all(data)?;
        file.set_len((offset + data.len()) as u64)
    }
}
