use fxhash::FxHasher;
use std::hash::Hasher;

use crate::ObjectId;

pub fn compute_hash(level: u32, object_id: &ObjectId, replica_num: u32, idx: usize) -> u32 {
    let mut h = FxHasher::default();
    h.write_u32(level);
    h.write(&object_id.0);
    h.write_u32(replica_num);
    h.write_u32(idx as u32);
    let r: u64 = h.finish();
    return r as u32
}
