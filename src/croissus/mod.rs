use crate::command::Command;
use crate::core::{NodeId, Packet, VirtualNetwork};
use crate::croissus::flow::Flow;
use anyhow::{bail, Result};
use futures::future::err;
use log::{debug, error, info};
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::hash_map::Entry::Vacant;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot::Sender;
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub mod flow;

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
struct Proposal {
    command: Command,
    proposer: NodeId,
    flow: Flow,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
enum ProposalSlot {
    None,
    Tombstone,
    Proposal(Proposal),
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
struct LockedState {
    proposal: ProposalSlot,
    echoes: HashMap<NodeId, HashSet<NodeId>>,
    acks: HashSet<NodeId>,
    done: bool,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
enum Message {
    Diffuse(Proposal),
    Echo(Proposal),
    Ack(Proposal), // Does an ack need to be identified?
    Lock,
    LockReply(LockedState),
}

struct CroissusState {
    node: NodeId,
    node_flow: Flow,
    majority_threshold: usize,
    proposal: ProposalSlot,
    echoes: HashMap<NodeId, HashSet<NodeId>>,
    acks: HashSet<NodeId>,
    done: bool,
    finish_adopt_commit_phase: Option<oneshot::Sender<()>>,
    finish_lock_phase: Option<oneshot::Sender<Option<Command>>>,
    fetched_states: HashMap<NodeId, LockedState>,
}

impl CroissusState {
    fn new(node: NodeId, node_flow: Flow, nodes: usize) -> Self {
        CroissusState {
            node,
            node_flow,
            majority_threshold: nodes / 2 + 1,
            proposal: ProposalSlot::None,
            echoes: HashMap::new(),
            acks: HashSet::new(),
            done: false,
            finish_adopt_commit_phase: None,
            finish_lock_phase: None,
            fetched_states: HashMap::new(),
        }
    }

    fn can_ack(&self) -> bool {
        match &self.proposal {
            ProposalSlot::None | ProposalSlot::Tombstone => false, //TODO: You cannot ack if you've got a TOMBSTONE
            ProposalSlot::Proposal(proposal) => {
                // The variable should_have_echoed from the pseudocode is a set of nodes which
                // should echo the current node. Since echoes are symmetrical, we can just use
                // the set of nodes that the current node echoes. Then, this set of nodes needs to
                // be a subset of the echoes the node has received for the proposal's proposer.
                // (Only 1 proposal can be sent by a proposer at a certain slot)
                if let Some(echoes) = self.echoes.get(&proposal.proposer) {
                    if !proposal.flow.echo_to[&self.node]
                        .difference(echoes)
                        .next()
                        .is_none()
                    {
                        return false;
                    }
                }

                // All nodes which the current node forwards the proposal must ack
                // before the current node can ack.
                if !proposal.flow.diffuse_to[&self.node]
                    .difference(&self.acks)
                    .next()
                    .is_none()
                {
                    return false;
                }

                //TODO: Have the timeout deadline embedded in the proposal

                true
            }
        }
    }

    fn locked(&self) -> bool {
        match self.proposal {
            ProposalSlot::None => false,
            ProposalSlot::Tombstone | ProposalSlot::Proposal(_) => true,
        }
    }

    fn lock(&mut self) -> LockedState {
        if !self.locked() {
            self.proposal = ProposalSlot::Tombstone;
        }

        LockedState {
            proposal: self.proposal.clone(),
            echoes: self.echoes.clone(),
            acks: self.acks.clone(),
            done: self.done,
        }
    }

    fn propose(&mut self, command: Command) -> (Proposal, oneshot::Receiver<()>) {
        let proposal = Proposal {
            command,
            proposer: self.node,
            flow: self.node_flow.clone(),
        };

        self.proposal = ProposalSlot::Proposal(proposal.clone());

        let (send, recv) = oneshot::channel();
        self.finish_adopt_commit_phase.replace(send);

        (proposal, recv)
    }

    fn go_to_lock_phase(&mut self) -> oneshot::Receiver<Option<Command>> {
        let (finish_lock_phase, lock_phase_finished) = oneshot::channel();
        self.finish_lock_phase.replace(finish_lock_phase);
        self.fetched_states.clear();

        lock_phase_finished
    }

    fn process_diffuse(&mut self, proposal: Proposal) -> Result<()> {
        if self.locked() {
            bail!("Node {} is locked, aborting diffuse", self.node); // TODO: Send NACK optimization
        }
        self.proposal = ProposalSlot::Proposal(proposal);

        Ok(())
    }

    fn try_ack(&mut self, proposal: Proposal) -> Option<(NodeId, Message)> {
        if self.can_ack() {
            self.done = true;
            let predecessor = proposal
                .flow
                .diffuse_to
                .iter()
                .find(|(predecessor, diffuses_to)| diffuses_to.contains(&self.node))
                .map(|(predecessor, _)| *predecessor);
            return predecessor.map(|predecessor| (predecessor, Message::Ack(proposal)));
        }

        None
    }

    fn process_lock_reply(&mut self, from: NodeId, locked_state: LockedState) {
        self.fetched_states.insert(from, locked_state);

        if self.fetched_states.len() == self.majority_threshold {
            let (deduced_count, fetched_count, mut proposed_commands) = self.deduce();

            let mut command = None;
            for (proposer, fetched) in fetched_count {
                if fetched + deduced_count[&proposer] >= self.majority_threshold {
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
            match &state.proposal {
                ProposalSlot::None => {
                    error!(
                        "Node {} fetched {:?} from {}",
                        self.node, state.proposal, node
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

struct Croissus {
    network: VirtualNetwork<Message>,
    state: Arc<Mutex<CroissusState>>,
    timeout: Duration,
    cancellation_token: CancellationToken,
}

impl Croissus {
    pub fn new(
        network: VirtualNetwork<Message>,
        nodes: Vec<NodeId>,
        rtt: Duration,
        receive_channel: Receiver<Packet<Message>>,
    ) -> (Arc<Self>, JoinHandle<()>) {
        let node = network.node;

        let croissus = Arc::new(Croissus {
            network,
            state: Arc::new(Mutex::new(CroissusState::new(
                node,
                Flow::ring(nodes.clone(), node),
                nodes.len(),
            ))),
            timeout: rtt,
            cancellation_token: CancellationToken::new(),
        });

        let croissus_clone = croissus.clone();
        let recv_handle = tokio::spawn(async move {
            croissus_clone.receive_loop(receive_channel).await;
        });

        (croissus, recv_handle)
    }

    pub fn quit(&self) {
        self.cancellation_token.cancel()
    }

    pub async fn propose(&self, command: Command) -> Result<()> {
        match self.try_commit(command).await {
            Ok(_) => Ok(()),
            Err(_) => {
                println!("ADOPTED {:?}", self.get_safe_value().await);
                Ok(())
            }
        }
    }

    async fn try_commit(&self, command: Command) -> Result<()> {
        let (proposal, finished_propose) = {
            let mut state = self.state.lock().await;
            if state.locked() {
                error!("Node {} state is locked, cannot propose", self.network.node);
                bail!("Nodes state is locked, it must adopt");
            }
            state.propose(command.clone())
        };

        let timeout = tokio::time::sleep(self.timeout);

        self.diffuse_proposal(proposal).await;
        select! {
            _ = timeout => {
                debug!("Node {} ack timeout elapsed", self.network.node);
            }
            _ = finished_propose => {}
        }

        {
            let mut state = self.state.lock().await;
            if state.done {
                println!("COMMITTED {:?}", command);
                return Ok(());
            }
        }

        bail!("Node couldn't commit, it must adopt")
    }

    async fn diffuse_proposal(&self, proposal: Proposal) {
        for destination in &proposal.flow.diffuse_to[&self.network.node] {
            debug!(
                "Node {} sending diffuse {:?} to {}",
                self.network.node, proposal, destination
            );
            self.network
                .send(*destination, Message::Diffuse(proposal.clone()))
                .await
                .unwrap()
        }
    }

    async fn get_safe_value(&self) -> Option<Command> {
        let lock_phase_finished = {
            let mut state = self.state.lock().await;
            state.go_to_lock_phase()
        };

        self.network.broadcast(Message::Lock).await.unwrap();

        let result = lock_phase_finished.await;
        if let Err(error) = result {
            error!("Node {} failed to finish lock phase, sender part of channel - finish_lock_phase dropped. Error: {}", self.network.node, error);
            panic!("{error}");
        }
        result.unwrap()
    }

    async fn receive_loop(&self, mut recv_channel: Receiver<Packet<Message>>) {
        debug!("Node {} initialized recv loop", self.network.node);
        loop {
            select! {
                result = recv_channel.recv() => {
                    if result.is_none() {
                        info!("Receive loop receiving channel closed");
                        return;
                    }

                    let packet = result.unwrap();
                    match packet.data {
                        Message::Diffuse(proposal) => self.process_diffuse(packet.from, proposal).await,
                        Message::Echo(proposal) => self.process_echo(packet.from, proposal).await,
                        Message::Ack(proposal) => self.process_ack(packet.from, proposal).await,
                        Message::Lock => self.process_lock(packet.from).await,
                        Message::LockReply(locked_state) => self.process_lock_reply(packet.from, locked_state).await
                    };
                }
                _ = self.cancellation_token.cancelled() => {
                    info!("Terminated receive loop");
                    return;
                }
            }
        }
    }

    async fn process_diffuse(&self, from: NodeId, proposal: Proposal) {
        debug!(
            "Node {} received diffuse {:?} from {}",
            self.network.node, proposal, from
        );
        let mut state = self.state.lock().await;

        let diffuse_result = state.process_diffuse(proposal.clone());
        if let Err(error) = diffuse_result {
            debug!("{error}");
            return; // TODO: Return negative ack
        }

        for sibling in &proposal.flow.echo_to[&self.network.node] {
            debug!(
                "Node {} sending echo {:?} to sibling {}",
                self.network.node, proposal, sibling
            );
            self.network
                .send(*sibling, Message::Echo(proposal.clone()))
                .await
                .unwrap();
        }

        self.diffuse_proposal(proposal.clone()).await;

        if let Some((destination, message)) = state.try_ack(proposal) {
            debug!("Node {} sending ack to {}", self.network.node, destination);
            self.network.send(destination, message).await.unwrap()
        }
    }

    async fn process_echo(&self, from: NodeId, proposal: Proposal) {
        debug!(
            "Node {} received echo {:?} from {}",
            self.network.node, proposal, from
        );
        let mut state = self.state.lock().await;
        let echoes = state.echoes.entry(proposal.proposer).or_default();
        echoes.insert(from);

        // TODO: Is it possible for 2 proposal's from different nodes to have the same echo?
        if let Some((destination, message)) = state.try_ack(proposal) {
            debug!("Node {} sending ack to {}", self.network.node, destination);
            self.network.send(destination, message).await.unwrap()
        }
    }

    async fn process_ack(&self, from: NodeId, proposal: Proposal) {
        debug!(
            "Node {} received ack {:?} from {}",
            self.network.node, proposal, from
        );
        let mut state = self.state.lock().await;
        state.acks.insert(from);

        if self.network.node == proposal.proposer {
            if state.can_ack() {
                state.done = true;
                match state.finish_adopt_commit_phase.take() {
                    None => {
                        error!(
                            "Node {} never set the send part for finish_propose",
                            self.network.node
                        );
                    }
                    Some(finish_propose) => {
                        if let Err(_) = finish_propose.send(()) {
                            debug!("Node {} recv part of finish_propose channel close - probably due to timeout", self.network.node);
                        }
                    }
                };
            }
        } else if let Some((destination, message)) = state.try_ack(proposal) {
            debug!("Node {} sending ack to {}", self.network.node, destination);
            self.network.send(destination, message).await.unwrap()
        }
    }

    async fn process_lock(&self, from: NodeId) {
        debug!("Node {} received lock from {}", self.network.node, from);
        let mut state = self.state.lock().await;
        let locked_state = state.lock();

        debug!(
            "Node {} sending lock reply {:?} to {}",
            self.network.node, locked_state, from
        );
        self.network
            .send(from, Message::LockReply(locked_state))
            .await
            .unwrap()
    }

    async fn process_lock_reply(&self, from: NodeId, locked_state: LockedState) {
        debug!(
            "Node {} received lock reply {:?} from {}",
            self.network.node, locked_state, from
        );
        let mut state = self.state.lock().await;
        state.process_lock_reply(from, locked_state);
    }
}

#[cfg(test)]
mod tests {
    use crate::command::{Command, CommandKind, WriteCommand};
    use crate::core::{create_channel_network, NodeId};
    use crate::croissus::Croissus;
    use std::collections::HashMap;
    use std::env::set_var;
    use std::time::Duration;

    #[tokio::test]
    async fn test_croissus() {
        unsafe {
            set_var("RUST_LOG", "TRACE");
        }
        pretty_env_logger::init_timed();
        let nodes = (0..9).into_iter().collect::<Vec<_>>();
        let (mut virtual_networks, mut receivers) = create_channel_network(nodes.clone());

        let mut croissuses = HashMap::new();
        let mut spawned_tasks = Vec::new();
        for node in &nodes {
            let network = virtual_networks.remove(node).unwrap();
            let receive_channel = receivers.remove(node).unwrap();
            let (croissus, join_handle) = Croissus::new(
                network,
                nodes.clone(),
                Duration::from_millis(1000),
                receive_channel,
            );
            croissuses.insert(node, croissus);
            spawned_tasks.push(join_handle);
        }

        croissuses[&4]
            .propose(Command {
                id: 1,
                client_id: 69,
                command_kind: CommandKind::Write(WriteCommand { key: 1, value: 10 }),
            })
            .await
            .unwrap();

        croissuses[&3]
            .propose(Command {
                id: 1,
                client_id: 69,
                command_kind: CommandKind::Write(WriteCommand { key: 1, value: 10 }),
            })
            .await
            .unwrap();

        assert!(false);
    }
}
