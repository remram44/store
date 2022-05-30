use std::fmt::Debug;

#[derive(Clone)]
pub struct DeviceId(pub [u8; 16]);

#[derive(Clone, Hash)]
pub struct ObjectId(pub [u8; 16]);

pub fn print_u8_16(f: &mut std::fmt::Formatter, array: &[u8; 16]) -> std::fmt::Result {
    write!(f, "{:02x}", array[0])?;
    for b in &array[1..] {
        write!(f, ":{:02x}", b)?;
    }
    Ok(())
}

impl Debug for DeviceId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DeviceId(")?;
        print_u8_16(f, &self.0)?;
        write!(f, ")")
    }
}

impl Debug for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ObjectId(")?;
        print_u8_16(f, &self.0)?;
        write!(f, ")")
    }
}

pub enum Node {
    Device(DeviceId),
    Bucket(Bucket),
}

pub struct Bucket {
    pub algorithm: Algorithm,
    pub children: Vec<NodeEntry>,
}

pub struct NodeEntry {
    pub weight: u32,
    pub node: Node,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Algorithm {
    Uniform,
    Straw,
    List,
}

fn compute_hash(level: u32, object_id: &ObjectId, replica_num: u32, idx: usize) -> u32 {
    use std::hash::{Hash, Hasher};

    let mut h = std::collections::hash_map::DefaultHasher::new();
    (level, object_id, replica_num, idx).hash(&mut h);
    let r: u64 = h.finish();
    return r as u32
}

pub fn compute_location(node: &Node, object_id: &ObjectId, replica_id: u32) -> DeviceId {
    compute_location_level(node, object_id, replica_id, 0)
}

fn draw_straw(object_id: &ObjectId, replica_num: u32, level: u32, idx: usize, weight: u32) -> u32 {
    let hash = compute_hash(level, object_id, replica_num, idx);
    (hash % 0x1000) * weight
}

fn compute_location_level(node: &Node, object_id: &ObjectId, replica_num: u32, level: u32) -> DeviceId {
    match node {
        &Node::Device(ref id) => id.clone(),
        &Node::Bucket(ref bucket) => {
            match bucket.algorithm {
                Algorithm::Uniform => {
                    // Hash the input
                    let hash = compute_hash(level, object_id, replica_num, 0);

                    // Pick the entry
                    let index = hash as usize % bucket.children.len();
                    compute_location_level(&bucket.children[index].node, object_id, replica_num, level + 1)
                }
                Algorithm::List => {
                    // Compute total weight
                    let total_weight: u32 = bucket.children.iter().map(|e| e.weight).sum();

                    // Draw
                    let mut hash = compute_hash(level, object_id, replica_num, 0) % total_weight;
                    for (i, child) in bucket.children[0..bucket.children.len() - 1].iter().enumerate() {
                        if hash < child.weight {
                            return compute_location_level(&bucket.children[i].node, object_id, replica_num, level + 1);
                        }
                        hash -= child.weight;
                    }
                    compute_location_level(&bucket.children[bucket.children.len() - 1].node, object_id, replica_num, level + 1)
                }
                Algorithm::Straw => {
                    // Draw straws for every entry, scaled by the weight
                    // This means "weight" is NOT the relative probability, though it can be
                    // computed from the intended probabilities
                    let mut best = 0;
                    let mut best_straw = draw_straw(object_id, replica_num, level, 0, bucket.children[0].weight);
                    for i in 1..bucket.children.len() {
                        let straw = draw_straw(object_id, replica_num, level, i, bucket.children[i].weight);
                        if straw > best_straw {
                            best = i;
                            best_straw = straw;
                        }
                    }

                    compute_location_level(&bucket.children[best].node, object_id, replica_num, level + 1)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;
    use super::{Algorithm, Bucket, DeviceId, Node, NodeEntry, ObjectId, compute_location};

    #[test]
    fn test_deviceid_debug() {
        let id = DeviceId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        let mut repr = String::new();
        write!(&mut repr, "{:?}", id).unwrap();
        assert_eq!(repr, "DeviceId(01:02:03:04:05:06:07:08:09:0a:0b:0c:0d:0e:0f:10)");
    }

    fn assert_frequencies(counts: &[usize], target: &[f32]) {
        assert_eq!(counts.len(), target.len());
        let total: usize = counts.iter().sum();
        let frequencies: Vec<f32> = counts.iter().map(|&c| c as f32 / total as f32).collect();
        assert!(
            frequencies.iter().enumerate().all(|(i, f)| f - 0.01 <= target[i] && f + 0.01 >= target[i]),
            "{:.2?} != {:.2?}",
            frequencies,
            target,
        );
    }

    #[test]
    fn test_uniform() {
        let root = Node::Bucket(
            Bucket {
                algorithm: Algorithm::Uniform,
                children: vec![
                    NodeEntry {
                        weight: 1,
                        node: Node::Device(DeviceId([1; 16])),
                    },
                    NodeEntry {
                        weight: 1,
                        node: Node::Device(DeviceId([2; 16])),
                    },
                    NodeEntry {
                        weight: 1,
                        node: Node::Device(DeviceId([3; 16])),
                    },
                ],
            }
        );
        let target = [0.333, 0.333, 0.333];

        let mut counts = [0; 3];
        const NUM: usize = 100000;
        for i in 0..NUM {
            let mut object = [0; 16];
            object[15] = i as u8;
            object[14] = (i >> 8) as u8;
            object[13] = (i >> 16) as u8;
            object[12] = (i >> 24) as u8;
            let device = compute_location(&root, &ObjectId(object), 0);
            counts[device.0[0] as usize - 1] += 1;
        }

        assert_frequencies(&counts, &target);
    }

    #[test]
    fn test_list() {
        let root = Node::Bucket(
            Bucket {
                algorithm: Algorithm::List,
                children: vec![
                    NodeEntry {
                        weight: 4,
                        node: Node::Device(DeviceId([1; 16])),
                    },
                    NodeEntry {
                        weight: 3,
                        node: Node::Device(DeviceId([2; 16])),
                    },
                    NodeEntry {
                        weight: 1,
                        node: Node::Device(DeviceId([3; 16])),
                    },
                    NodeEntry {
                        weight: 2,
                        node: Node::Device(DeviceId([4; 16])),
                    },
                ],
            }
        );
        let target = [0.4, 0.3, 0.1, 0.2];

        let mut counts = [0; 4];
        const NUM: usize = 100000;
        for i in 0..NUM {
            let mut object = [0; 16];
            object[15] = i as u8;
            object[14] = (i >> 8) as u8;
            object[13] = (i >> 16) as u8;
            object[12] = (i >> 24) as u8;
            let device = compute_location(&root, &ObjectId(object), 0);
            counts[device.0[0] as usize - 1] += 1;
        }

        assert_frequencies(&counts, &target);
    }

    #[test]
    fn test_straw() {
        let root = Node::Bucket(
            Bucket {
                algorithm: Algorithm::Straw,
                children: vec![
                    NodeEntry {
                        weight: 659,
                        node: Node::Device(DeviceId([1; 16])),
                    },
                    NodeEntry {
                        weight: 900,
                        node: Node::Device(DeviceId([2; 16])),
                    },
                    NodeEntry {
                        weight: 1000,
                        node: Node::Device(DeviceId([3; 16])),
                    },
                    NodeEntry {
                        weight: 794,
                        node: Node::Device(DeviceId([4; 16])),
                    },
                ],
            }
        );
        let target = [0.1, 0.3, 0.4, 0.2];

        let mut counts = [0; 4];
        const NUM: usize = 100000;
        for i in 0..NUM {
            let mut object = [0; 16];
            object[15] = i as u8;
            object[14] = (i >> 8) as u8;
            object[13] = (i >> 16) as u8;
            object[12] = (i >> 24) as u8;
            let device = compute_location(&root, &ObjectId(object), 0);
            counts[device.0[0] as usize - 1] += 1;
        }

        assert_frequencies(&counts, &target);
    }
}
