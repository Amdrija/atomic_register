use crate::core::NodeId;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct Flow {
    pub diffuse_to: HashMap<NodeId, HashSet<NodeId>>,
    pub echo_to: HashMap<NodeId, HashSet<NodeId>>,
}

impl Flow {
    // Here, it is assumed that neighbouring nodes in the vector are the closest ones in the ring
    // Also, the total nodes with the proposer and the nodes in both branches should be equal to
    // a majority and the branches need to be of equal length. This means that given k the number
    // of nodes in a branch, majority = 1 + 2 * k = N / 2, where N is the number of nodes.
    // As a result, N = 4 * k + 1, k = 2, 3... (odd numbers are better for fault tolerance,
    // but, in theory, numbers in form of N = 4 * k are also valid, I just didn't put much
    // thought into this scenario).
    pub fn ring(nodes: Vec<NodeId>, proposer: NodeId) -> Flow {
        // if there are 9 nodes, then we need 4 nodes more for majority along with the proposer
        // So, the length of the branch is nodes.len() / 4 = 2.
        let branch_len = nodes.len() / 4;
        let mut left_branch = Vec::new();
        let mut right_branch = Vec::new();
        let (proposer_idx, _) = nodes
            .iter()
            .enumerate()
            .find(|(_, node)| **node == proposer)
            .unwrap();

        for i in 0..branch_len {
            let next_right_idx = (proposer_idx + i + 1) % nodes.len();
            right_branch.push(nodes[next_right_idx]);

            let next_left_idx = if proposer_idx < i + 1 {
                // equivalent to proposer_idx - i - 1 < 0
                nodes.len() + proposer_idx - i - 1
            } else {
                proposer_idx - i - 1
            };
            left_branch.push(nodes[next_left_idx]);
        }

        let mut diffuse_to = HashMap::new();
        diffuse_to.insert(
            proposer,
            vec![left_branch[0], right_branch[0]].into_iter().collect(),
        );

        for i in 0..branch_len - 1 {
            diffuse_to.insert(
                left_branch[i],
                vec![left_branch[i + 1]].into_iter().collect(),
            );
            diffuse_to.insert(
                right_branch[i],
                vec![right_branch[i + 1]].into_iter().collect(),
            );
        }
        diffuse_to.insert(left_branch[branch_len - 1], HashSet::new());
        diffuse_to.insert(right_branch[branch_len - 1], HashSet::new());

        let mut echo_to = HashMap::new();
        echo_to.insert(proposer, HashSet::new());
        for (sibling1, sibling2) in left_branch[0..branch_len - 1]
            .iter()
            .zip(right_branch[0..branch_len - 1].iter().rev())
        {
            echo_to.insert(*sibling1, vec![*sibling2].into_iter().collect());
            echo_to.insert(*sibling2, vec![*sibling1].into_iter().collect());
        }
        echo_to.insert(left_branch[branch_len - 1], HashSet::new());
        echo_to.insert(right_branch[branch_len - 1], HashSet::new());

        Flow {
            diffuse_to,
            echo_to,
        }
    }

    fn vec_to_set(vector: Vec<NodeId>) -> HashSet<NodeId> {
        vector.into_iter().collect()
    }

    // Enumerates the replicas that have adopted a proposal if we know it was adopted by p
    pub fn adoptions_given_adopted(&self, process: NodeId) -> HashSet<NodeId> {
        let mut adopted = HashSet::new();
        adopted.insert(process);

        for (src, _) in self
            .diffuse_to
            .iter()
            .filter(|(_, dsts)| dsts.contains(&process))
        {
            adopted.insert(*src);
            adopted.extend(&self.adoptions_given_adopted(*src));
        }

        adopted
    }

    // Enumerates the replicas that have adopted a proposal if we know it was acked by p
    pub fn adoptions_given_acked(&self, process: NodeId) -> HashSet<NodeId> {
        let mut adopted = self.adoptions_given_adopted(process);

        // Since echo_to is symmetrical, you can just use self.echo_to[process]
        for (echoer, _) in self
            .echo_to
            .iter()
            .filter(|(_, dsts)| dsts.contains(&process))
        {
            adopted.extend(self.adoptions_given_adopted(*echoer));
        }

        for dst in &self.diffuse_to[&process] {
            adopted.extend(self.adoptions_given_acked(*dst));
        }

        adopted
    }
}

#[cfg(test)]
mod test {
    use crate::core::NodeId;
    use crate::croissus::flow::Flow;
    use std::collections::HashSet;

    #[test]
    fn test_ring_flow_9_nodes() {
        let flow = Flow::ring((0..9).into_iter().collect::<Vec<_>>(), 4);

        assert_eq!(
            flow.diffuse_to[&4],
            vec![3, 5].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.diffuse_to[&5],
            vec![6].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(flow.diffuse_to[&6], HashSet::<NodeId>::new());
        assert_eq!(
            flow.diffuse_to[&3],
            vec![2].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(flow.diffuse_to[&2], HashSet::<NodeId>::new());

        assert_eq!(flow.echo_to[&4], HashSet::<NodeId>::new());
        assert_eq!(
            flow.echo_to[&5],
            vec![3].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(flow.echo_to[&6], HashSet::<NodeId>::new());
        assert_eq!(
            flow.echo_to[&3],
            vec![5].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(flow.echo_to[&2], HashSet::<NodeId>::new());
    }

    #[test]
    fn test_ring_flow_13_nodes() {
        let flow = Flow::ring((0..13).into_iter().collect::<Vec<_>>(), 6);

        assert_eq!(
            flow.diffuse_to[&6],
            vec![5, 7].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.diffuse_to[&7],
            vec![8].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.diffuse_to[&8],
            vec![9].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(flow.diffuse_to[&9], HashSet::<NodeId>::new());
        assert_eq!(
            flow.diffuse_to[&5],
            vec![4].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.diffuse_to[&4],
            vec![3].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(flow.diffuse_to[&3], HashSet::<NodeId>::new());

        assert_eq!(flow.echo_to[&6], HashSet::<NodeId>::new());
        assert_eq!(
            flow.echo_to[&7],
            vec![4].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.echo_to[&8],
            vec![5].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(flow.echo_to[&9], HashSet::<NodeId>::new());
        assert_eq!(
            flow.echo_to[&5],
            vec![8].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.echo_to[&4],
            vec![7].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(flow.echo_to[&3], HashSet::<NodeId>::new());
    }

    #[test]
    fn test_ring_flow_13_nodes_negative_overflow() {
        let flow = Flow::ring((0..13).into_iter().collect::<Vec<_>>(), 0);

        assert_eq!(
            flow.diffuse_to[&0],
            vec![12, 1].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.diffuse_to[&1],
            vec![2].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.diffuse_to[&2],
            vec![3].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(flow.diffuse_to[&3], HashSet::<NodeId>::new());
        assert_eq!(
            flow.diffuse_to[&12],
            vec![11].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.diffuse_to[&11],
            vec![10].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(flow.diffuse_to[&10], HashSet::<NodeId>::new());

        assert_eq!(flow.echo_to[&0], HashSet::<NodeId>::new());
        assert_eq!(
            flow.echo_to[&1],
            vec![11].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.echo_to[&2],
            vec![12].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(flow.echo_to[&3], HashSet::<NodeId>::new());
        assert_eq!(
            flow.echo_to[&12],
            vec![2].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.echo_to[&11],
            vec![1].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(flow.echo_to[&10], HashSet::<NodeId>::new());
    }

    #[test]
    fn test_ring_flow_9_nodes_positive_overflow() {
        let flow = Flow::ring((0..9).into_iter().collect::<Vec<_>>(), 8);

        assert_eq!(
            flow.diffuse_to[&8],
            vec![7, 0].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.diffuse_to[&0],
            vec![1].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(flow.diffuse_to[&1], HashSet::<NodeId>::new());
        assert_eq!(
            flow.diffuse_to[&7],
            vec![6].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(flow.diffuse_to[&6], HashSet::<NodeId>::new());

        assert_eq!(flow.echo_to[&8], HashSet::<NodeId>::new());
        assert_eq!(
            flow.echo_to[&0],
            vec![7].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(flow.echo_to[&1], HashSet::<NodeId>::new());
        assert_eq!(
            flow.echo_to[&7],
            vec![0].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(flow.echo_to[&6], HashSet::<NodeId>::new());
    }

    #[test]
    fn test_ring_13_adopted() {
        let flow = Flow::ring((0..13).into_iter().collect::<Vec<_>>(), 6);

        assert_eq!(
            flow.adoptions_given_adopted(3),
            vec![3, 4, 5, 6].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.adoptions_given_adopted(4),
            vec![4, 5, 6].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.adoptions_given_adopted(5),
            vec![5, 6].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.adoptions_given_adopted(9),
            vec![9, 8, 7, 6].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.adoptions_given_adopted(8),
            vec![8, 7, 6].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.adoptions_given_adopted(7),
            vec![7, 6].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.adoptions_given_adopted(6),
            vec![6].into_iter().collect::<HashSet<NodeId>>()
        );
    }

    #[test]
    fn test_ring_9_adopted() {
        let flow = Flow::ring((0..9).into_iter().collect::<Vec<_>>(), 4);

        assert_eq!(
            flow.adoptions_given_adopted(2),
            vec![2, 3, 4].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.adoptions_given_adopted(3),
            vec![3, 4].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.adoptions_given_adopted(6),
            vec![6, 5, 4].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.adoptions_given_adopted(5),
            vec![5, 4].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.adoptions_given_adopted(4),
            vec![4].into_iter().collect::<HashSet<NodeId>>()
        );
    }

    #[test]
    fn test_ring_13_acked() {
        let flow = Flow::ring((0..13).into_iter().collect::<Vec<_>>(), 6);

        assert_eq!(
            flow.adoptions_given_acked(3),
            vec![3, 4, 5, 6].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.adoptions_given_acked(4),
            vec![3, 4, 5, 6, 7].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.adoptions_given_acked(5),
            vec![3, 4, 5, 6, 8, 7]
                .into_iter()
                .collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.adoptions_given_acked(9),
            vec![9, 8, 7, 6].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.adoptions_given_acked(8),
            vec![9, 8, 7, 6, 5].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.adoptions_given_acked(7),
            vec![9, 8, 7, 6, 4, 5]
                .into_iter()
                .collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.adoptions_given_acked(6),
            vec![3, 4, 5, 6, 7, 8, 9]
                .into_iter()
                .collect::<HashSet<NodeId>>()
        );
    }

    #[test]
    fn test_ring_9_acked() {
        let flow = Flow::ring((0..9).into_iter().collect::<Vec<_>>(), 4);

        assert_eq!(
            flow.adoptions_given_acked(2),
            vec![2, 3, 4].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.adoptions_given_acked(3),
            vec![2, 3, 4, 5].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.adoptions_given_acked(6),
            vec![6, 5, 4].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.adoptions_given_acked(5),
            vec![6, 5, 4, 3].into_iter().collect::<HashSet<NodeId>>()
        );
        assert_eq!(
            flow.adoptions_given_acked(4),
            vec![2, 3, 4, 5, 6].into_iter().collect::<HashSet<NodeId>>()
        );
    }
}
