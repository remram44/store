use log::info;
use rand::{Rng, thread_rng};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::io::Error as IoError;
use std::sync::{Arc, Mutex};

use crate::{DeviceId, ObjectId, PoolName};
use super::StorageBackend;

#[derive(Default)]
struct InnerStore(HashMap<PoolName, HashMap<ObjectId, Vec<u8>>>);

/// A storage backend keeping all data in memory, in a HashMap.
///
/// This is NOT persistent, the data will be lost when the process ends or the
/// MemStore object is dropped.
#[derive(Clone, Default)]
pub struct MemStore(Arc<Mutex<InnerStore>>);

impl StorageBackend for MemStore {
    fn read_object(&self, pool: &PoolName, object_id: ObjectId) -> Result<Option<Vec<u8>>, IoError> {
        let store = self.0.lock().unwrap();
        let object = store.0.get(pool).and_then(|p| p.get(&object_id));
        Ok(object.cloned())
    }

    fn read_part(&self, pool: &PoolName, object_id: ObjectId, offset: usize, len: usize) -> Result<Option<Vec<u8>>, IoError> {
        let store = self.0.lock().unwrap();
        let object = store.0.get(pool).and_then(|p| p.get(&object_id));
        let part = object.map(|o| o[o.len().min(offset)..o.len().min(offset + len)].to_owned());
        Ok(part)
    }

    fn write_object(&self, pool: &PoolName, object_id: ObjectId, data: &[u8]) -> Result<(), IoError> {
        let mut store = self.0.lock().unwrap();
        let pool = store.0.entry(pool.to_owned()).or_default();
        pool.insert(object_id, data.to_owned());
        Ok(())
    }

    fn write_part(&self, pool: &PoolName, object_id: ObjectId, offset: usize, data: &[u8]) -> Result<(), IoError> {
        let mut store = self.0.lock().unwrap();
        let pool = store.0.entry(pool.to_owned()).or_default();
        match pool.entry(object_id.to_owned()) {
            Entry::Occupied(mut e) => {
                let value = e.get_mut();
                value.resize(value.len().max(offset + data.len()), 0);
                value[offset..offset + data.len()].clone_from_slice(data);
            }
            Entry::Vacant(e) => {
                let mut value = Vec::with_capacity(offset + data.len());
                value.resize(offset, 0);
                value.extend_from_slice(data);
                e.insert(value);
            }
        }
        Ok(())
    }

    fn delete_object(&self, pool: &PoolName, object_id: ObjectId) -> Result<(), IoError> {
        let mut store = self.0.lock().unwrap();
        store.0.get_mut(pool).map(|p| p.remove(&object_id));
        Ok(())
    }
}

pub fn create_mem_store() -> (MemStore, DeviceId) {
    // Generate a random device ID
    let mut rng = thread_rng();
    let mut bytes = [0; 16];
    rng.fill(&mut bytes);
    let device_id = DeviceId(bytes);
    info!("Generated ID: {:?}", device_id);

    (MemStore::default(), device_id)
}

#[cfg(test)]
mod tests {
    use super::MemStore;

    #[test]
    fn test_memstore_common() {
        let storage = MemStore::default();
        super::super::test_backend(storage);
    }
}
