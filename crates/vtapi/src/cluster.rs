use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use thiserror::Error;

const DEFAULT_FAILURE_BACKOFF: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    storage_nodes: Vec<String>,
    control_nodes: Vec<String>,
    local_control_node: Option<String>,
    topology_groups: HashMap<String, String>,
    node_weights: HashMap<String, u32>,
    replication_factor: usize,
    write_quorum: usize,
    read_quorum: usize,
    failure_backoff: Duration,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ClusterConfigError {
    #[error("cluster requires at least one storage node")]
    EmptyStorageNodes,
    #[error("cluster replication factor must be greater than zero")]
    InvalidReplicationFactor,
    #[error(
        "cluster replication factor {replication_factor} exceeds storage node count {node_count}"
    )]
    ReplicationFactorTooLarge {
        replication_factor: usize,
        node_count: usize,
    },
    #[error("cluster write quorum {write_quorum} exceeds replication factor {replication_factor}")]
    WriteQuorumTooLarge {
        write_quorum: usize,
        replication_factor: usize,
    },
    #[error("cluster read quorum {read_quorum} exceeds replication factor {replication_factor}")]
    ReadQuorumTooLarge {
        read_quorum: usize,
        replication_factor: usize,
    },
}

impl ClusterConfig {
    pub fn new(
        storage_nodes: Vec<String>,
        replication_factor: usize,
    ) -> Result<Self, ClusterConfigError> {
        let storage_nodes: Vec<String> = storage_nodes
            .into_iter()
            .map(|node| node.trim().trim_end_matches('/').to_string())
            .filter(|node| !node.is_empty())
            .collect();

        if storage_nodes.is_empty() {
            return Err(ClusterConfigError::EmptyStorageNodes);
        }
        if replication_factor == 0 {
            return Err(ClusterConfigError::InvalidReplicationFactor);
        }
        if replication_factor > storage_nodes.len() {
            return Err(ClusterConfigError::ReplicationFactorTooLarge {
                replication_factor,
                node_count: storage_nodes.len(),
            });
        }

        Ok(Self {
            storage_nodes,
            control_nodes: Vec::new(),
            local_control_node: None,
            topology_groups: HashMap::new(),
            node_weights: HashMap::new(),
            replication_factor,
            write_quorum: replication_factor,
            read_quorum: 1,
            failure_backoff: DEFAULT_FAILURE_BACKOFF,
        })
    }

    pub fn storage_nodes(&self) -> &[String] {
        &self.storage_nodes
    }

    pub fn replication_factor(&self) -> usize {
        self.replication_factor
    }

    pub fn control_nodes(&self) -> &[String] {
        if self.control_nodes.is_empty() {
            self.local_control_node
                .as_ref()
                .map(std::slice::from_ref)
                .unwrap_or(&[])
        } else {
            &self.control_nodes
        }
    }

    pub fn local_control_node(&self) -> Option<&str> {
        self.local_control_node.as_deref()
    }

    pub fn write_quorum(&self) -> usize {
        self.write_quorum
    }

    pub fn failure_backoff(&self) -> Duration {
        self.failure_backoff
    }

    pub fn read_quorum(&self) -> usize {
        self.read_quorum
    }

    pub fn topology_group(&self, node: &str) -> Option<&str> {
        self.topology_groups.get(node).map(String::as_str)
    }

    pub fn node_weight(&self, node: &str) -> u32 {
        self.node_weights.get(node).copied().unwrap_or(1)
    }

    pub fn with_write_quorum(mut self, write_quorum: usize) -> Result<Self, ClusterConfigError> {
        if write_quorum == 0 {
            return Err(ClusterConfigError::InvalidReplicationFactor);
        }
        if write_quorum > self.replication_factor {
            return Err(ClusterConfigError::WriteQuorumTooLarge {
                write_quorum,
                replication_factor: self.replication_factor,
            });
        }

        self.write_quorum = write_quorum;
        Ok(self)
    }

    pub fn with_failure_backoff(mut self, failure_backoff: Duration) -> Self {
        self.failure_backoff = failure_backoff;
        self
    }

    pub fn with_control_nodes(mut self, control_nodes: Vec<String>) -> Self {
        self.control_nodes = control_nodes
            .into_iter()
            .map(|node| node.trim().trim_end_matches('/').to_string())
            .filter(|node| !node.is_empty())
            .collect();
        self
    }

    pub fn with_local_control_node(mut self, local_control_node: impl Into<String>) -> Self {
        let local_control_node = local_control_node.into();
        let local_control_node = local_control_node.trim().trim_end_matches('/').to_string();
        if local_control_node.is_empty() {
            self.local_control_node = None;
        } else {
            self.local_control_node = Some(local_control_node);
        }
        self
    }

    pub fn with_topology_groups(mut self, topology_groups: HashMap<String, String>) -> Self {
        self.topology_groups = topology_groups
            .into_iter()
            .filter_map(|(node, group)| {
                let node = node.trim().trim_end_matches('/').to_string();
                let group = group.trim().to_string();
                if node.is_empty()
                    || group.is_empty()
                    || !self
                        .storage_nodes
                        .iter()
                        .any(|storage_node| storage_node == &node)
                {
                    None
                } else {
                    Some((node, group))
                }
            })
            .collect();
        self
    }

    pub fn with_node_weights(mut self, node_weights: HashMap<String, u32>) -> Self {
        self.node_weights = node_weights
            .into_iter()
            .filter_map(|(node, weight)| {
                let node = node.trim().trim_end_matches('/').to_string();
                if node.is_empty()
                    || weight == 0
                    || !self
                        .storage_nodes
                        .iter()
                        .any(|storage_node| storage_node == &node)
                {
                    None
                } else {
                    Some((node, weight))
                }
            })
            .collect();
        self
    }

    pub fn with_read_quorum(mut self, read_quorum: usize) -> Result<Self, ClusterConfigError> {
        if read_quorum == 0 {
            return Err(ClusterConfigError::InvalidReplicationFactor);
        }
        if read_quorum > self.replication_factor {
            return Err(ClusterConfigError::ReadQuorumTooLarge {
                read_quorum,
                replication_factor: self.replication_factor,
            });
        }

        self.read_quorum = read_quorum;
        Ok(self)
    }

    pub fn placements(&self, trace_id: &str) -> Vec<&str> {
        let mut candidate_indexes: Vec<usize> = (0..self.storage_nodes.len()).collect();
        candidate_indexes.sort_by(|left, right| {
            let left_score = self.placement_score(trace_id, *left);
            let right_score = self.placement_score(trace_id, *right);
            left_score
                .partial_cmp(&right_score)
                .expect("placement score should be comparable")
                .then_with(|| self.storage_nodes[*left].cmp(&self.storage_nodes[*right]))
        });
        let mut selected_indexes = Vec::with_capacity(self.replication_factor);
        let mut used_topology_groups: HashSet<&str> = HashSet::new();

        for index in candidate_indexes.iter().copied() {
            if selected_indexes.len() >= self.replication_factor {
                break;
            }
            match self
                .topology_groups
                .get(&self.storage_nodes[index])
                .map(String::as_str)
            {
                Some(group) if used_topology_groups.contains(group) => continue,
                Some(group) => {
                    used_topology_groups.insert(group);
                    selected_indexes.push(index);
                }
                None => selected_indexes.push(index),
            }
        }

        for index in candidate_indexes {
            if selected_indexes.len() >= self.replication_factor {
                break;
            }
            if selected_indexes.contains(&index) {
                continue;
            }
            if let Some(group) = self
                .topology_groups
                .get(&self.storage_nodes[index])
                .map(String::as_str)
            {
                used_topology_groups.insert(group);
            }
            selected_indexes.push(index);
        }

        selected_indexes
            .into_iter()
            .map(|index| self.storage_nodes[index].as_str())
            .collect()
    }

    pub fn elect_control_leader<'a, I>(&self, healthy_nodes: I) -> Option<String>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut healthy_nodes: Vec<String> = healthy_nodes
            .into_iter()
            .map(|node| node.trim().trim_end_matches('/').to_string())
            .filter(|node| !node.is_empty())
            .collect();
        healthy_nodes.sort();
        healthy_nodes.dedup();

        let eligible: HashSet<String> = self.control_nodes().iter().cloned().collect();
        healthy_nodes
            .into_iter()
            .filter(|node| eligible.contains(node))
            .min()
    }

    pub fn is_local_control_leader<'a, I>(&self, healthy_nodes: I) -> bool
    where
        I: IntoIterator<Item = &'a str>,
    {
        let Some(local_control_node) = self.local_control_node() else {
            return false;
        };
        self.elect_control_leader(healthy_nodes)
            .as_deref()
            .map(|leader| leader == local_control_node)
            .unwrap_or(false)
    }

    fn placement_score(&self, trace_id: &str, index: usize) -> f64 {
        let node = &self.storage_nodes[index];
        let weight = self.node_weights.get(node).copied().unwrap_or(1).max(1) as f64;
        let hash = stable_hash_pair(trace_id, node);
        let uniform = ((hash as f64) + 1.0) / ((u64::MAX as f64) + 2.0);
        -uniform.ln() / weight
    }
}

fn stable_hash(value: &str) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;

    let mut hash = OFFSET_BASIS;
    for byte in value.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

fn stable_hash_pair(left: &str, right: &str) -> u64 {
    mix_hash64(
        stable_hash(left)
            ^ stable_hash(right)
                .rotate_left(32)
                .wrapping_add(0x9e3779b97f4a7c15),
    )
}

fn mix_hash64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58476d1ce4e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::{ClusterConfig, ClusterConfigError};

    #[test]
    fn placements_are_stable_and_cover_replication_factor() {
        let config = ClusterConfig::new(
            vec![
                "http://node-a".to_string(),
                "http://node-b".to_string(),
                "http://node-c".to_string(),
            ],
            2,
        )
        .expect("cluster config");

        let left = config.placements("trace-a");
        let right = config.placements("trace-a");

        assert_eq!(left, right);
        assert_eq!(left.len(), 2);
        assert_ne!(left[0], left[1]);
        assert_eq!(config.write_quorum(), 2);
        assert_eq!(config.read_quorum(), 1);
    }

    #[test]
    fn rejects_invalid_replication_factor() {
        let error = ClusterConfig::new(vec!["http://node-a".to_string()], 2)
            .expect_err("config should fail");

        assert_eq!(
            error,
            ClusterConfigError::ReplicationFactorTooLarge {
                replication_factor: 2,
                node_count: 1,
            }
        );
    }

    #[test]
    fn rejects_write_quorum_larger_than_replication_factor() {
        let error = ClusterConfig::new(
            vec!["http://node-a".to_string(), "http://node-b".to_string()],
            2,
        )
        .expect("cluster config")
        .with_write_quorum(3)
        .expect_err("quorum should fail");

        assert_eq!(
            error,
            ClusterConfigError::WriteQuorumTooLarge {
                write_quorum: 3,
                replication_factor: 2,
            }
        );
    }

    #[test]
    fn rejects_read_quorum_larger_than_replication_factor() {
        let error = ClusterConfig::new(
            vec!["http://node-a".to_string(), "http://node-b".to_string()],
            2,
        )
        .expect("cluster config")
        .with_read_quorum(3)
        .expect_err("read quorum should fail");

        assert_eq!(
            error,
            ClusterConfigError::ReadQuorumTooLarge {
                read_quorum: 3,
                replication_factor: 2,
            }
        );
    }

    #[test]
    fn placements_spread_across_topology_groups_when_available() {
        let config = ClusterConfig::new(
            vec![
                "http://node-a".to_string(),
                "http://node-b".to_string(),
                "http://node-c".to_string(),
            ],
            2,
        )
        .expect("cluster config")
        .with_topology_groups(HashMap::from([
            ("http://node-a".to_string(), "az-a".to_string()),
            ("http://node-b".to_string(), "az-b".to_string()),
            ("http://node-c".to_string(), "az-a".to_string()),
        ]));

        let placements = config.placements("trace-topology-a");

        assert_eq!(placements.len(), 2);
        assert_ne!(placements[0], placements[1]);
        let topology_groups: HashSet<&str> = placements
            .iter()
            .map(|node| {
                config
                    .topology_groups
                    .get(*node)
                    .map(String::as_str)
                    .unwrap()
            })
            .collect();
        assert_eq!(topology_groups.len(), 2);
    }

    #[test]
    fn placements_prefer_higher_weight_nodes() {
        let config = ClusterConfig::new(
            vec![
                "http://node-a".to_string(),
                "http://node-b".to_string(),
                "http://node-c".to_string(),
            ],
            1,
        )
        .expect("cluster config")
        .with_node_weights(HashMap::from([
            ("http://node-a".to_string(), 10),
            ("http://node-b".to_string(), 1),
            ("http://node-c".to_string(), 1),
        ]));

        let mut counts: HashMap<String, usize> = HashMap::new();
        for index in 0..4_096 {
            let node = config.placements(&format!("trace-weighted-{index}"))[0].to_string();
            *counts.entry(node).or_default() += 1;
        }

        let node_a = counts.get("http://node-a").copied().unwrap_or(0);
        let node_b = counts.get("http://node-b").copied().unwrap_or(0);
        let node_c = counts.get("http://node-c").copied().unwrap_or(0);
        assert!(node_a > node_b);
        assert!(node_a > node_c);
        assert!(node_a > node_b + node_c);
    }

    #[test]
    fn placements_limit_key_movement_when_node_is_added() {
        let before = ClusterConfig::new(
            vec![
                "http://node-a".to_string(),
                "http://node-b".to_string(),
                "http://node-c".to_string(),
            ],
            1,
        )
        .expect("cluster config");
        let after = ClusterConfig::new(
            vec![
                "http://node-a".to_string(),
                "http://node-b".to_string(),
                "http://node-c".to_string(),
                "http://node-d".to_string(),
            ],
            1,
        )
        .expect("cluster config");

        let moved = (0..4_096)
            .filter(|index| {
                let trace_id = format!("trace-move-{index}");
                before.placements(&trace_id)[0] != after.placements(&trace_id)[0]
            })
            .count();

        assert!(moved < 1_800, "too many trace placements moved: {moved}");
    }

    #[test]
    fn elects_smallest_healthy_control_node_as_leader() {
        let config = ClusterConfig::new(
            vec![
                "http://storage-a".to_string(),
                "http://storage-b".to_string(),
            ],
            1,
        )
        .expect("cluster config")
        .with_control_nodes(vec![
            "http://select-b".to_string(),
            "http://select-a".to_string(),
            "http://select-c".to_string(),
        ])
        .with_local_control_node("http://select-b");

        let leader = config
            .elect_control_leader(["http://select-c", "http://select-b", "http://select-a"])
            .expect("leader");
        assert_eq!(leader, "http://select-a");
        assert!(!config.is_local_control_leader([
            "http://select-c",
            "http://select-b",
            "http://select-a",
        ]));
    }

    #[test]
    fn elects_local_control_node_when_it_is_only_healthy_peer() {
        let config = ClusterConfig::new(
            vec![
                "http://storage-a".to_string(),
                "http://storage-b".to_string(),
            ],
            1,
        )
        .expect("cluster config")
        .with_control_nodes(vec![
            "http://select-a".to_string(),
            "http://select-b".to_string(),
        ])
        .with_local_control_node("http://select-b");

        let leader = config
            .elect_control_leader(["http://select-b"])
            .expect("leader");
        assert_eq!(leader, "http://select-b");
        assert!(config.is_local_control_leader(["http://select-b"]));
    }
}
