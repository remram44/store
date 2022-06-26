pub mod mem_store;
#[cfg(feature = "rocksdb")]
pub mod rocksdb_store;

use std::io::Error as IoError;

use crate::{ObjectId, PoolName};

pub trait StorageBackend: Send + Sync {
    /// Reads a whole object.
    fn read_object(&self, pool: &PoolName, object_id: &ObjectId) -> Result<Option<Vec<u8>>, IoError>;

    /// Reads part of an object.
    fn read_part(&self, pool: &PoolName, object_id: &ObjectId, offset: usize, len: usize) -> Result<Option<Vec<u8>>, IoError>;

    /// Write a whole object.
    fn write_object(&self, pool: &PoolName, object_id: &ObjectId, data: &[u8]) -> Result<(), IoError>;

    /// Overwrite part of an object.
    fn write_part(&self, pool: &PoolName, object_id: &ObjectId, offset: usize, data: &[u8]) -> Result<(), IoError>;

    /// Delete an object.
    fn delete_object(&self, pool: &PoolName, object_id: &ObjectId) -> Result<(), IoError>;
}

#[cfg(test)]
fn test_backend<S: StorageBackend>(storage: S) {
    let pool1 = PoolName("mapoule".to_owned());
    let obj1 = ObjectId((b"greeting" as &[u8]).to_owned());
    let obj2 = ObjectId((b"other" as &[u8]).to_owned());
    let obj3 = ObjectId((b"maybe" as &[u8]).to_owned());

    // Write whole object
    storage.write_object(&pool1, &obj1, b"hello world!").unwrap();
    assert_eq!(
        storage
            .read_object(&pool1, &obj1)
            .unwrap()
            .as_deref(),
        Some(b"hello world!" as &[u8])
    );

    // Write part into new object
    storage.write_part(&pool1, &obj2, 5, b"hi").unwrap();
    assert_eq!(
        storage
            .read_object(&pool1, &obj2)
            .unwrap()
            .as_deref(),
        Some(b"\x00\x00\x00\x00\x00hi" as &[u8])
    );

    // Write part into existing object
    storage.write_part(&pool1, &obj1, 3, b"xxx").unwrap();
    assert_eq!(
        storage
            .read_object(&pool1, &obj1)
            .unwrap()
            .as_deref(),
        Some(b"helxxxworld!" as &[u8])
    );

    // Write part past end of existing object
    storage.write_part(&pool1, &obj1, 10, b"!!!").unwrap();
    assert_eq!(
        storage
            .read_object(&pool1, &obj1)
            .unwrap()
            .as_deref(),
        Some(b"helxxxworl!!!" as &[u8])
    );

    // Read part of object
    assert_eq!(
        storage
            .read_part(&pool1, &obj1, 4, 3)
            .unwrap()
            .as_deref(),
        Some(b"xxw" as &[u8])
    );

    // Read too big a part
    assert_eq!(
        storage
            .read_part(&pool1, &obj1, 4, 20)
            .unwrap()
            .as_deref(),
        Some(b"xxworl!!!" as &[u8])
    );

    // Read fully outside part
    assert_eq!(
        storage
            .read_part(&pool1, &obj1, 20, 20)
            .unwrap()
            .as_deref(),
        Some(b"" as &[u8])
    );

    // Read non-existent object
    assert_eq!(storage.read_object(&pool1, &obj3).unwrap(), None);
    assert_eq!(storage.read_part(&pool1, &obj3, 3, 2).unwrap(), None);
}
