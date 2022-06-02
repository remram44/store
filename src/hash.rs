use fxhash::FxHasher;
use std::hash::Hasher;

use crate::{GroupId, ObjectId};

pub fn compute_hash(level: u32, group_id: &GroupId, replica_num: u32, idx: usize) -> u32 {
    let mut h = FxHasher::default();
    h.write_u32(level);
    h.write_u32(group_id.0);
    h.write_u32(replica_num);
    h.write_u32(idx as u32);
    let r: u64 = h.finish();
    r as u32
}

pub fn compute_object_hash(object_id: &ObjectId) -> u32 {
    let mut h = FxHasher::default();
    h.write(&object_id.0);
    let r: u64 = h.finish();
    r as u32
}
