pub mod client;
pub mod crypto;
pub mod daemon;
mod hash;
pub mod master;
pub mod metrics;
pub mod proto;
pub mod storage;
pub mod storage_map;

use std::fmt::Debug;

/// The ID of a device, which also identifies the storage daemon for it.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DeviceId(pub [u8; 16]);

/// The name of a storage pool.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PoolName(pub String);

/// The name of an object, which can be freely picked by clients.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ObjectId(pub Vec<u8>);

/// The ID for a group of objects.
///
/// Objects are assembled into groups using hashes. The procedure depends on
/// the current number of groups, which changes over time.
#[derive(Clone, PartialEq, Eq)]
pub struct GroupId(pub u32);

impl Debug for DeviceId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DeviceId({:02x}", self.0[0])?;
        for b in &self.0[1..] {
            write!(f, ":{:02x}", b)?;
        }
        write!(f, ")")
    }
}

impl Debug for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ObjectId({})", String::from_utf8_lossy(&self.0))
    }
}

impl Debug for GroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "GroupId(0x{:04x})", self.0)
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;
    use super::DeviceId;

    #[test]
    fn test_deviceid_debug() {
        let id = DeviceId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        let mut repr = String::new();
        write!(&mut repr, "{:?}", id).unwrap();
        assert_eq!(
            repr,
            "DeviceId(01:02:03:04:05:06:07:08:09:0a:0b:0c:0d:0e:0f:10)"
        );
    }
}
