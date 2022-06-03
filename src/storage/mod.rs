pub mod daemon;
mod file_store;

use std::io::Error as IoError;

use crate::{ObjectId, PoolName};

pub trait StorageBackend: Send + Sync {
    /// Reads a whole object.
    fn read_object(&self, pool: &PoolName, object_id: ObjectId) -> Result<Option<Vec<u8>>, IoError>;

    /// Reads part of an object.
    fn read_part(&self, pool: &PoolName, object_id: ObjectId, offset: usize, len: usize) -> Result<Option<Vec<u8>>, IoError>;

    /// Write a whole object.
    fn write_object(&self, pool: &PoolName, object_id: ObjectId, data: &[u8]) -> Result<(), IoError>;

    /// Overwrite part of an object.
    fn write_part(&self, pool: &PoolName, object_id: ObjectId, offset: usize, data: &[u8]) -> Result<(), IoError>;

    /// Delete an object.
    fn delete_object(&self, pool: &PoolName, object_id: ObjectId) -> Result<(), IoError>;
}
