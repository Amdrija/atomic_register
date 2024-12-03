use crate::command::Command;
use crate::core;
use crate::core::{NodeId, Packet};
use crate::croissus::flow::Flow;
use crate::croissus::messages::{AckMessage, DiffuseMessage, EchoMessage, MessageKind};
use anyhow::bail;
use log::{debug, error};
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::hash_map::Entry::Vacant;
use std::collections::{HashMap, HashSet};
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct Proposal {
    pub command: Command,
    pub proposer: NodeId,
    pub flow: Flow,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub enum ProposalSlot {
    None,
    Tombstone,
    Proposal(Proposal),
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct LockedState {
    pub proposal_slot: ProposalSlot,
    pub echoes: HashMap<NodeId, HashSet<NodeId>>,
    pub acks: HashSet<NodeId>,
    pub done: bool,
}

impl LockedState {
    fn new(proposal_slot: ProposalSlot) -> Self {
        Self {
            proposal_slot,
            echoes: HashMap::new(),
            acks: HashSet::new(),
            done: false,
        }
    }
}

#[derive(Debug)]
pub enum CroissusResult {
    Committed(Command),
    Adopted(Option<Command>),
}

impl CroissusResult {
    pub fn is_committed(&self) -> bool {
        match self {
            CroissusResult::Committed(_) => true,
            _ => false,
        }
    }

    pub fn is_adopted(&self) -> bool {
        match self {
            CroissusResult::Adopted(_) => true,
            _ => false,
        }
    }
}

pub struct CroissusState {
    node: NodeId,
    majority_threshold: usize,
    nodes: Vec<NodeId>,
    node_flow: Flow,
    pub log: Vec<Option<LockedState>>,
    pub current_index: usize,
    pub finish_adopt_commit_phase: Option<Sender<()>>,
    finish_lock_phase: Option<Sender<Option<Command>>>,
    fetched_states: HashMap<NodeId, LockedState>,
}

impl CroissusState {
    pub fn new(node: NodeId, nodes: Vec<NodeId>, node_flow: Flow) -> Self {
        CroissusState {
            node,
            majority_threshold: nodes.len() / 2 + 1,
            nodes,
            node_flow,
            log: vec![Some(LockedState::new(ProposalSlot::Tombstone))],
            current_index: 0,
            finish_adopt_commit_phase: None,
            finish_lock_phase: None,
            fetched_states: HashMap::new(),
        }
    }

    pub fn propose(
        &mut self,
        command: Command,
    ) -> (Vec<Packet<MessageKind>>, usize, oneshot::Receiver<()>) {
        let proposal = Proposal {
            command,
            proposer: self.node,
            flow: self.node_flow.clone(),
        };

        self.current_index = self.first_unlocked_index();
        // TODO: It is possible that a slot for which the node receives an echo is overridden
        // However, this shouldn't impact the algorithm, as the echoed proposal will never be
        // echoed by this node (because it sets another proposal here, so it is locked). Therefore,
        // the node's sibling will never ack, so the sibling's echoed proposal will never
        // be committed. I think that also this proposal will never be committed as well.
        self.set_log(self.current_index, ProposalSlot::Proposal(proposal.clone()));

        let (send, recv) = oneshot::channel();
        self.finish_adopt_commit_phase.replace(send);

        (
            self.make_diffuse_packets(DiffuseMessage {
                index: self.current_index,
                proposal,
            }),
            self.current_index,
            recv,
        )
    }

    fn make_diffuse_packets(&self, diffuse: DiffuseMessage) -> Vec<Packet<MessageKind>> {
        diffuse.proposal.flow.diffuse_to[&self.node]
            .iter()
            .map(|to| Packet {
                from: self.node,
                to: *to,
                data: MessageKind::Diffuse(diffuse.clone()),
            })
            .collect()
    }

    pub fn is_propose_committed(&self) -> bool {
        match self
            .log
            .get(self.current_index)
            .and_then(|slot| slot.as_ref())
        {
            None => {
                error!(
                    "Node {} couldn't find a log slot for the current index: {}",
                    self.node, self.current_index
                );
                false
            }
            Some(slot) => slot.done,
        }
    }

    pub fn process_diffuse(&mut self, diffuse: DiffuseMessage) -> Vec<Packet<MessageKind>> {
        if self.locked(diffuse.index) {
            debug!("Node {} is locked, aborting diffuse", self.node); // TODO: Send NACK optimization
            return Vec::new();
        }

        // TODO: It is possible that a slot for which the node receives an echo is overridden
        // However, this shouldn't impact the algorithm, as the echoed proposal will never be
        // echoed by this node (because it sets another proposal here, so it is locked). Therefore,
        // the node's sibling will never ack, so the sibling's echoed proposal will never
        // be committed. I think that also this proposal will never be committed as well.
        self.set_log(
            diffuse.index,
            ProposalSlot::Proposal(diffuse.proposal.clone()),
        );

        let mut packets = Vec::new();
        packets.extend(
            diffuse.proposal.flow.echo_to[&self.node]
                .iter()
                .map(|sibling| Packet {
                    from: self.node,
                    to: *sibling,
                    data: MessageKind::Echo(EchoMessage {
                        index: diffuse.index,
                        proposal: diffuse.proposal.clone(),
                    }),
                }),
        );
        packets.extend(self.make_diffuse_packets(diffuse.clone()));

        if let Some((destination, message)) = self.try_ack(diffuse.index, diffuse.proposal) {
            packets.push(Packet {
                from: self.node,
                to: destination,
                data: message,
            });
        }

        packets
    }

    pub fn can_ack(&self, index: usize) -> bool {
        if index >= self.log.len() {
            // The log currently doesn't have anything at the index, so there's nothing to ack
            // Shouldn't be possible to trigger with the way algorithm works
            return false;
        }

        match &self.log[index] {
            None => false, // If nothing at index then nothing that can be acked
            Some(locked_state) => {
                if locked_state.done {
                    return false;
                }

                match &locked_state.proposal_slot {
                    ProposalSlot::None | ProposalSlot::Tombstone => false, //TODO: You cannot ack if you've got a TOMBSTONE
                    ProposalSlot::Proposal(proposal) => {
                        // The variable should_have_echoed from the pseudocode is a set of nodes which
                        // should echo the current node. Since echoes are symmetrical, we can just use
                        // the set of nodes that the current node echoes. Then, this set of nodes needs to
                        // be a subset of the echoes the node has received for the proposal's proposer.
                        // (Only 1 proposal can be sent by a proposer at a certain slot)
                        if !proposal.flow.echo_to[&self.node]
                            .difference(
                                &locked_state
                                    .echoes
                                    .get(&proposal.proposer)
                                    .cloned()
                                    .unwrap_or_default(),
                            )
                            .next()
                            .is_none()
                        {
                            return false;
                        }

                        // All nodes which the current node forwards the proposal must ack
                        // before the current node can ack.
                        if !proposal.flow.diffuse_to[&self.node]
                            .difference(&locked_state.acks)
                            .next()
                            .is_none()
                        {
                            return false;
                        }

                        true
                    }
                }
            }
        }
    }

    pub fn locked(&self, index: usize) -> bool {
        if index >= self.log.len() {
            return false;
        }

        match &self.log[index] {
            None => false,
            Some(locked_state) => match locked_state.proposal_slot {
                ProposalSlot::None => false,
                ProposalSlot::Tombstone | ProposalSlot::Proposal(_) => true,
            },
        }
    }

    pub fn lock(&mut self, index: usize) -> LockedState {
        if !self.locked(index) {
            self.set_log(index, ProposalSlot::Tombstone);
        }

        match &self.log[index] {
            None => {
                error!(
                    "Node {} tried to lock index {} with empty slot",
                    self.node, index
                );
                panic!(
                    "Node {} tried to lock index {} with empty slot",
                    self.node, index
                );
            }
            Some(locked_state) => locked_state.clone(),
        }
    }

    pub fn set_log(&mut self, index: usize, proposal_slot: ProposalSlot) {
        if index >= self.log.len() {
            self.log.resize(index + 1, None);
        }
        self.log[index] = Some(LockedState::new(proposal_slot));
    }

    fn first_unlocked_index(&self) -> usize {
        // Basically, it is possible to have a slot here which doesn't have a proposal,
        // but it has received an echo from a sibling
        (0..self.log.len())
            .find(|index| !self.locked(*index))
            .unwrap_or(self.log.len())
    }

    pub fn go_to_lock_phase(&mut self) -> (usize, oneshot::Receiver<Option<Command>>) {
        let (finish_lock_phase, lock_phase_finished) = oneshot::channel();
        self.finish_lock_phase.replace(finish_lock_phase);
        self.fetched_states.clear();

        (self.current_index, lock_phase_finished)
    }

    pub fn process_echo(
        &mut self,
        from: NodeId,
        index: usize,
        proposal: Proposal,
    ) -> Option<(NodeId, MessageKind)> {
        if index >= self.log.len() {
            self.set_log(index, ProposalSlot::None);
        }

        let echoes = self.log[index]
            .as_mut()
            .unwrap()
            .echoes
            .entry(proposal.proposer)
            .or_default();
        echoes.insert(from);
        self.try_ack(index, proposal)
    }

    // TODO: Refactor so this also returns a vector of packets
    pub fn try_ack(&mut self, index: usize, proposal: Proposal) -> Option<(NodeId, MessageKind)> {
        if self.can_ack(index) {
            self.log[index].as_mut().unwrap().done = true;
            let predecessor = proposal
                .flow
                .diffuse_to
                .iter()
                .find(|(_, diffuses_to)| diffuses_to.contains(&self.node))
                .map(|(predecessor, _)| *predecessor);
            return predecessor.map(|predecessor| {
                (
                    predecessor,
                    MessageKind::Ack(AckMessage { index, proposal }),
                )
            });
        }

        None
    }

    pub fn process_lock_reply(&mut self, from: NodeId, locked_state: LockedState) {
        self.fetched_states.insert(from, locked_state);

        if self.fetched_states.len() == self.majority_threshold {
            let (deduced_count, fetched_count, mut proposed_commands) = self.deduce();

            let mut command = None;
            for (proposer, fetched) in fetched_count {
                if fetched + deduced_count.get(&proposer).cloned().unwrap_or_default()
                    >= self.majority_threshold
                {
                    command = proposed_commands.remove(&proposer);
                    break;
                }
            }

            if command.is_none() {
                for (proposer, deduced) in deduced_count {
                    if deduced >= self.majority_threshold / 2 {
                        command = proposed_commands.remove(&proposer);
                        break;
                    }
                }
            }

            match self.finish_lock_phase.take() {
                None => {
                    error!("The finish_lock_phase channel must have been set beforehand.");
                }
                Some(finish_lock) => finish_lock.send(command).unwrap(),
            }
        }
    }

    fn deduce(
        &self,
    ) -> (
        HashMap<NodeId, usize>,
        HashMap<NodeId, usize>,
        HashMap<NodeId, Command>,
    ) {
        let mut known = self.fetched_states.keys().cloned().collect::<HashSet<_>>();
        let mut fetched_count = HashMap::new();
        let mut deduced_count = HashMap::new();
        let mut proposed_commands = HashMap::new();

        for (node, state) in &self.fetched_states {
            match &state.proposal_slot {
                ProposalSlot::None => {
                    error!(
                        "Node {} fetched {:?} from {}",
                        self.node, state.proposal_slot, node
                    );
                    panic!(
                        "Node {} responded to Lock message with {:?}",
                        node,
                        ProposalSlot::None
                    );
                    //TODO: A nice way would be to have a different enum here, but that's just
                    // more boilerplate
                }
                ProposalSlot::Tombstone => {
                    continue;
                }
                ProposalSlot::Proposal(proposal) => {
                    if let Vacant(entry) = proposed_commands.entry(proposal.proposer) {
                        entry.insert(proposal.command.clone());
                    }

                    let entry = fetched_count.entry(proposal.proposer).or_default();
                    *entry += 1;
                    if !state.done {
                        continue;
                    }

                    for node_adopted in proposal.flow.adoptions_given_acked(*node) {
                        if !known.contains(&node_adopted) {
                            known.insert(node_adopted);
                            let entry = deduced_count.entry(proposal.proposer).or_default();
                            *entry += 1;
                        }
                    }
                }
            }
        }

        (deduced_count, fetched_count, proposed_commands)
    }
}
