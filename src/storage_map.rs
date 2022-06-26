use std::collections::HashSet;

use crate::{DeviceId, GroupId, ObjectId};
use crate::hash::{compute_hash, compute_object_hash};

/// The configuration for a storage pool.
///
/// This contains the tree used to map a group to a device, as well as the
/// current number of groups.
pub struct StorageMap {
    pub generation: u32,
    pub groups: usize,
    pub replicas: u32,
    pub map_root: Node,
}

impl StorageMap {
    pub fn object_to_group(&self, object_id: &ObjectId) -> GroupId {
        let h = compute_object_hash(object_id);
        GroupId(h % self.groups as u32)
    }

    /// Gets the devices handling the given object group, in order.
    pub fn group_to_devices(&self, group_id: &GroupId, replicas: usize) -> Vec<DeviceId> {
        let mut devices = Vec::with_capacity(replicas);
        let mut already_picked = HashSet::new();
        for i in 0..replicas {
            match compute_location(&self.map_root, group_id, i as u32, 0, &mut already_picked) {
                Some(device) => devices.push(device),
                None => break,
            }
        }
        devices
    }

    /// Gets the first device handling the given object group.
    ///
    /// Shortcut for `group_to_devices.get(0)`
    pub fn group_to_first_device(&self, group_id: &GroupId) -> Option<DeviceId> {
        compute_location(&self.map_root, group_id, 0, 0, &mut HashSet::new())
    }
}

/// A node in the storage map.
#[derive(Clone, Debug)]
pub enum Node {
    Device(DeviceId),
    Bucket(Bucket),
}

/// Internal node in the storage map, allows picking one of multiple children.
#[derive(Clone, Debug)]
pub struct Bucket {
    pub id: u32,
    pub algorithm: Algorithm,
    pub pick_mode: PickMode,
    pub children: Vec<NodeEntry>,
}

#[derive(Clone, Copy, Debug)]
pub enum PickMode {
    /// Pseudo-random mode, pick whatever the hash function gives us.
    PseudoRandom,
    /// Don't pick the same child twice, fail instead.
    NeverRepeat,
}

#[derive(Clone, Debug)]
pub struct NodeEntry {
    pub weight: u32,
    pub node: Node,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Algorithm {
    Uniform,
    Straw(Vec<u32>),
    List,
    Fallback,
}

fn draw_straw(group_id: &GroupId, replica_num: u32, level: u32, attempt: u32, idx: usize, weight: u32) -> u32 {
    let hash = compute_hash(level, group_id, replica_num, attempt, idx);
    hash % weight
}

fn compute_location(node: &Node, group_id: &GroupId, replica_num: u32, level: u32, already_picked: &mut HashSet<(u32, u32)>) -> Option<DeviceId> {
    match node {
        &Node::Device(ref id) => Some(id.clone()),
        &Node::Bucket(ref bucket) => {
            let mut attempt = 0;
            loop {
                // Check that there are still children to be picked
                if let PickMode::NeverRepeat = bucket.pick_mode {
                    for i in 0..bucket.children.len() {
                        if already_picked.contains(&(bucket.id, i as u32)) {
                            return None;
                        }
                    }
                }

                // Compute location based on bucket's algorithm
                let index = compute_location_in_bucket(
                    bucket,
                    group_id,
                    replica_num,
                    level,
                    attempt,
                );

                // Avoid repeats by looping if child has already been picked
                if let PickMode::NeverRepeat = bucket.pick_mode {
                    // Mark it
                    let was_already_marked = !already_picked.insert((bucket.id, index as u32));

                    // Skip if already marked
                    if was_already_marked {
                        attempt += 1;
                        continue;
                    }
                }

                // Recursively process that child
                if let Some(device) = compute_location(
                    &bucket.children[index].node,
                    group_id,
                    replica_num,
                    level + 1,
                    already_picked,
                ) {
                    return Some(device);
                }

                attempt += 1;
            }
        }
    }
}

fn compute_location_in_bucket(bucket: &Bucket, group_id: &GroupId, replica_num: u32, level: u32, attempt: u32) -> usize {
    match bucket.algorithm {
        Algorithm::Uniform => {
            // Hash the input
            let hash = compute_hash(level, group_id, replica_num, attempt, 0);

            // Pick the entry
            hash as usize % bucket.children.len()
        }
        Algorithm::List => {
            // Compute total weight
            let total_weight: u32 = bucket.children.iter().map(|e| e.weight).sum();

            // Draw
            let mut hash = compute_hash(level, group_id, replica_num, attempt, 0) % total_weight;
            for (i, child) in bucket.children[0..bucket.children.len() - 1].iter().enumerate() {
                if hash < child.weight {
                    return i;
                }
                hash -= child.weight;
            }
            bucket.children.len() - 1
        }
        Algorithm::Straw(ref factors) => {
            // Draw straws for every entry, scaled by the factors
            let mut best = 0;
            let mut best_straw = draw_straw(group_id, replica_num, level, attempt, 0, factors[0]);
            for i in 1..bucket.children.len() {
                let straw = draw_straw(group_id, replica_num, level, attempt, i, factors[i]);
                if straw > best_straw {
                    best = i;
                    best_straw = straw;
                }
            }

            best
        }
        Algorithm::Fallback => {
            attempt as usize
        }
    }
}

pub fn build_straw_bucket(children: Vec<NodeEntry>, id: u32, pick_mode: PickMode) -> Bucket {
    // Sort weights from highest to lowest
    let mut order: Vec<usize> = (0..children.len()).collect();
    order.sort_by_key(|&i| -(children[i].weight as i32));

    // Turn given weights into probabilities
    let total: u32 = children.iter().map(|i| i.weight).sum();
    let probs: Vec<f64> = (0..children.len())
        .map(|i| children[order[i]].weight as f64 / total as f64)
        .collect();

    // Compute factors for desired probabilities
    let mut factors: Vec<u32> = vec![0; children.len()];
    factors[order[0]] = 0x100000;
    let mut mult = 1.0;
    for i in 1..children.len() {
        factors[order[i]] = (factors[order[i - 1]] as f32 * (1.0 - i as f32 * mult * (probs[i - 1] - probs[i]) as f32).powf(1.0 / i as f32)) as u32;
        mult *= (factors[order[i - 1]] as f32 / factors[order[i]] as f32).powf(i as f32);
    }

    Bucket {
        id,
        algorithm: Algorithm::Straw(factors),
        pick_mode,
        children: children,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use super::{Algorithm, Bucket, DeviceId, GroupId, Node, NodeEntry, ObjectId, PickMode, StorageMap, build_straw_bucket, compute_location};

    fn object_id(num: usize) -> ObjectId {
        ObjectId(vec![
            num as u8,
            (num >> 8) as u8,
            (num >> 16) as u8,
            (num >> 24) as u8,
        ])
    }

    fn assert_frequencies(counts: &[usize], target: &[f32]) {
        assert_eq!(counts.len(), target.len());
        let total: usize = counts.iter().sum();
        let frequencies: Vec<f32> = counts.iter().map(|&c| c as f32 / total as f32).collect();
        assert!(
            frequencies
                .iter()
                .enumerate()
                .all(|(i, f)| f - 0.01 <= target[i] && f + 0.01 >= target[i]),
            "{:.2?} != {:.2?}",
            frequencies,
            target,
        );
    }

    #[test]
    fn test_groups() {
        fn equal_1percent(a: usize, b: usize) -> bool {
            a * 100 >= b * 99 && a * 100 <= b * 101
        }

        const OBJECTS: usize = 100000;
        let objects: Vec<ObjectId> = (0..OBJECTS).into_iter().map(object_id).collect();

        // Map objects to groups
        const GROUPS1: usize = 128;
        let map1 = StorageMap {
            generation: 1,
            groups: GROUPS1,
            replicas: 1,
            map_root: Node::Device(DeviceId([1; 16])),
        };
        let mut group_counts1 = [0; GROUPS1];
        for obj in &objects {
            let group = map1.object_to_group(obj);
            group_counts1[group.0 as usize] += 1;
        }

        // Assert it is about even (2%)
        for &count in &group_counts1 {
            assert!(equal_1percent(count * GROUPS1, OBJECTS));
        }

        // Map objects to groups using a different number of groups
        const GROUPS2: usize = 256;
        let map2 = StorageMap {
            generation: 1,
            groups: GROUPS2,
            replicas: 1,
            map_root: Node::Device(DeviceId([1; 16])),
        };
        let mut group_counts2 = [0; GROUPS2];
        for obj in &objects {
            let group = map2.object_to_group(obj);
            group_counts2[group.0 as usize] += 1;
        }

        // Assert it is even also
        for &count in &group_counts2 {
            assert!(equal_1percent(count * GROUPS2, OBJECTS));
        }

        // Assert the number of differences
        let mut move_to_new = 0;
        let mut move_inner = 0;
        for obj in &objects {
            let group1 = map1.object_to_group(obj);
            let group2 = map2.object_to_group(obj);
            if group1 == group2 {
                // No movement
            } else if group2.0 as usize >= GROUPS1 {
                move_to_new += 1;
            } else {
                move_inner += 1;
            }
        }
        eprintln!("moved to new groups:{}", move_to_new);
        eprintln!("moved in old groups:{}", move_inner);
        assert_eq!(move_inner, 0);
        // About half the objects are moved
        assert!(equal_1percent(move_to_new * 2, OBJECTS));
    }

    #[test]
    fn test_uniform() {
        let root = Node::Bucket(
            Bucket {
                id: 0,
                algorithm: Algorithm::Uniform,
                pick_mode: PickMode::PseudoRandom,
                children: vec![
                    // Note that the weights do nothing
                    NodeEntry {
                        weight: 1,
                        node: Node::Device(DeviceId([1; 16])),
                    },
                    NodeEntry {
                        weight: 2,
                        node: Node::Device(DeviceId([2; 16])),
                    },
                    NodeEntry {
                        weight: 3,
                        node: Node::Device(DeviceId([3; 16])),
                    },
                ],
            }
        );
        let target = [0.333, 0.333, 0.333];

        let mut counts = [0; 3];
        const NUM: usize = 100000;
        for i in 0..NUM {
            let device = compute_location(&root, &GroupId(i as u32), 0, 0, &mut HashSet::new()).unwrap();
            counts[device.0[0] as usize - 1] += 1;
        }

        assert_frequencies(&counts, &target);
    }

    #[test]
    fn test_list() {
        let root = Node::Bucket(
            Bucket {
                id: 0,
                algorithm: Algorithm::List,
                pick_mode: PickMode::PseudoRandom,
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
            let device = compute_location(&root, &GroupId(i as u32), 0, 0, &mut HashSet::new()).unwrap();
            counts[device.0[0] as usize - 1] += 1;
        }

        assert_frequencies(&counts, &target);
    }

    #[test]
    fn test_straw() {
        let root = build_straw_bucket(
            vec![
                NodeEntry { weight: 1, node: Node::Device(DeviceId([1; 16])) },
                NodeEntry { weight: 3, node: Node::Device(DeviceId([2; 16])) },
                NodeEntry { weight: 4, node: Node::Device(DeviceId([3; 16])) },
                NodeEntry { weight: 2, node: Node::Device(DeviceId([4; 16])) },
            ],
            0,
            PickMode::PseudoRandom,
        );
        let factors = match root.algorithm {
            Algorithm::Straw(ref factors) => factors,
            _ => panic!("Invalid algorithm"),
        };
        assert_eq!(factors[0], 690648);
        assert_eq!(factors[1], 943718);
        assert_eq!(factors[2], 1048576);
        assert_eq!(factors[3], 832281);

        let root = Node::Bucket(root);
        let target = [0.1, 0.3, 0.4, 0.2];

        let mut counts = [0; 4];
        const NUM: usize = 1000000;
        for i in 0..NUM {
            let device = compute_location(&root, &GroupId(i as u32), 0, 0, &mut HashSet::new()).unwrap();
            counts[device.0[0] as usize - 1] += 1;
        }

        assert_frequencies(&counts, &target);
    }
}
