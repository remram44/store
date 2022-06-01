mod daemon;
mod file_store;

use std::io::Error as IoError;

use crate::{ObjectId};

pub trait StorageBackend {
    /// Reads a whole object
    fn read_object(&mut self, object_id: ObjectId) -> Result<Vec<u8>, IoError>;

    /// Reads part of an object
    fn read_part(&mut self, object_id: ObjectId, offset: usize, len: usize) -> Result<Vec<u8>, IoError>;

    /// Write a whole object
    fn write_object(&mut self, object_id: ObjectId, data: &[u8]) -> Result<(), IoError>;

    /// Overwrite part of an object
    fn write_part(&mut self, object_id: ObjectId, offset: usize, data: &[u8]) -> Result<(), IoError>;

    /// Delete an object
    fn delete_object(&mut self, object_id: ObjectId) -> Result<(), IoError>;
}
