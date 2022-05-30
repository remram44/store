use crate::{DeviceId, ObjectId};
use crate::hash::compute_hash;

#[derive(Clone, Debug)]
pub enum Node {
    Device(DeviceId),
    Bucket(Bucket),
}

#[derive(Clone, Debug)]
pub struct Bucket {
    pub algorithm: Algorithm,
    pub children: Vec<NodeEntry>,
}

#[derive(Clone, Debug)]
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

pub fn compute_location(node: &Node, object_id: &ObjectId, replica_id: u32) -> DeviceId {
    compute_location_level(node, object_id, replica_id, 0)
}

fn draw_straw(object_id: &ObjectId, replica_num: u32, level: u32, idx: usize, weight: u32) -> u32 {
    let hash = compute_hash(level, object_id, replica_num, idx);
    hash % weight
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

pub fn build_straw_bucket(input: &[(u32, Node)]) -> Bucket {
    // Sort weights from highest to lowest
    let mut order: Vec<usize> = (0..input.len()).collect();
    order.sort_by_key(|&i| -(input[i].0 as i32));

    // Turn given weights into probabilities
    let total: u32 = input.iter().map(|i| i.0).sum();
    let probs: Vec<f64> = (0..input.len()).map(|i| input[order[i]].0 as f64 / total as f64).collect();

    // Compute factors for desired probabilities
    let mut weights: Vec<u32> = vec![0; input.len()];
    weights[order[0]] = 0x100000;
    let mut mult = 1.0;
    for i in 1..input.len() {
        weights[order[i]] = (weights[order[i - 1]] as f32 * (1.0 - i as f32 * mult * (probs[i - 1] - probs[i]) as f32).powf(1.0 / i as f32)) as u32;
        mult *= (weights[order[i - 1]] as f32 / weights[order[i]] as f32).powf(i as f32);
    }

    // Build result
    let children = input.into_iter().enumerate().map(
        |(i, (_, c))| NodeEntry {
            weight: weights[i],
            node: c.clone(),
        }
    ).collect();
    Bucket {
        algorithm: Algorithm::Straw,
        children: children,
    }
}

#[cfg(test)]
mod tests {
    use super::{Algorithm, Bucket, DeviceId, Node, NodeEntry, ObjectId, build_straw_bucket, compute_location};

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
            let device = compute_location(&root, &object_id(i), 0);
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
            let device = compute_location(&root, &object_id(i), 0);
            counts[device.0[0] as usize - 1] += 1;
        }

        assert_frequencies(&counts, &target);
    }

    #[test]
    fn test_straw() {
        let root = build_straw_bucket(&[
            (1, Node::Device(DeviceId([1; 16]))),
            (3, Node::Device(DeviceId([2; 16]))),
            (4, Node::Device(DeviceId([3; 16]))),
            (2, Node::Device(DeviceId([4; 16]))),
        ]);
        assert_eq!(root.children[0].weight, 690648);
        assert_eq!(root.children[1].weight, 943718);
        assert_eq!(root.children[2].weight, 1048576);
        assert_eq!(root.children[3].weight, 832281);

        let root = Node::Bucket(root);
        let target = [0.1, 0.3, 0.4, 0.2];

        let mut counts = [0; 4];
        const NUM: usize = 1000000;
        for i in 0..NUM {
            let device = compute_location(&root, &object_id(i), 0);
            counts[device.0[0] as usize - 1] += 1;
        }

        assert_frequencies(&counts, &target);
    }
}
