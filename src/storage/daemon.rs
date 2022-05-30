use crate::DeviceId;
use super::StorageBackend;

pub struct StorageDaemon {
    storage: Box<dyn StorageBackend>,
    device_id: DeviceId,
}
