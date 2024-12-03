use crate::command::Command;
use crate::core::{NodeId, Packet, VirtualNetwork};
use crate::croissus::flow::Flow;
use crate::croissus::messages::{
    AckMessage, DiffuseMessage, EchoMessage, LockMessage, LockReplyMessage, MessageKind,
};
use anyhow::{anyhow, bail, Result};
use log::{debug, error, info};
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::hash_map::Entry::Vacant;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot::Sender;
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub mod flow;
mod messages;

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
    proposal_slot: ProposalSlot,
    echoes: HashMap<NodeId, HashSet<NodeId>>,
    acks: HashSet<NodeId>,
    done: bool,
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
enum CroissusResult {
    Committed(Command),
    Adopted(Option<Command>),
}

impl CroissusResult {
    fn is_committed(&self) -> bool {
        match self {
            CroissusResult::Committed(_) => true,
            _ => false,
        }
    }

    fn is_adopted(&self) -> bool {
        match self {
            CroissusResult::Adopted(_) => true,
            _ => false,
        }
    }
}

struct CroissusState {
    node: NodeId,
    node_flow: Flow,
    majority_threshold: usize,
    log: Vec<Option<LockedState>>,
    current_index: usize,
    finish_adopt_commit_phase: Option<Sender<()>>,
    finish_lock_phase: Option<Sender<Option<Command>>>,
    fetched_states: HashMap<NodeId, LockedState>,
}

impl CroissusState {
    fn new(node: NodeId, node_flow: Flow, nodes: usize) -> Self {
        CroissusState {
            node,
            node_flow,
            majority_threshold: nodes / 2 + 1,
            log: vec![Some(LockedState::new(ProposalSlot::Tombstone))],
            current_index: 0,
            finish_adopt_commit_phase: None,
            finish_lock_phase: None,
            fetched_states: HashMap::new(),
        }
    }

    fn can_ack(&self, index: usize) -> bool {
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

    fn locked(&self, index: usize) -> bool {
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

    fn lock(&mut self, index: usize) -> LockedState {
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

    fn set_log(&mut self, index: usize, proposal_slot: ProposalSlot) {
        if index >= self.log.len() {
            self.log.resize(index + 1, None);
        }
        self.log[index] = Some(LockedState::new(proposal_slot));
    }

    fn propose(&mut self, command: Command) -> (Proposal, usize, oneshot::Receiver<()>) {
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

        (proposal, self.current_index, recv)
    }

    fn first_unlocked_index(&self) -> usize {
        // Basically, it is possible to have a slot here which doesn't have a proposal,
        // but it has received an echo from a sibling
        (0..self.log.len())
            .find(|index| !self.locked(*index))
            .unwrap_or(self.log.len())
    }

    fn go_to_lock_phase(&mut self) -> (usize, oneshot::Receiver<Option<Command>>) {
        let (finish_lock_phase, lock_phase_finished) = oneshot::channel();
        self.finish_lock_phase.replace(finish_lock_phase);
        self.fetched_states.clear();

        (self.current_index, lock_phase_finished)
    }

    fn process_diffuse(&mut self, index: usize, proposal: Proposal) -> Result<()> {
        if self.locked(index) {
            bail!("Node {} is locked, aborting diffuse", self.node); // TODO: Send NACK optimization
        }

        // TODO: It is possible that a slot for which the node receives an echo is overridden
        // However, this shouldn't impact the algorithm, as the echoed proposal will never be
        // echoed by this node (because it sets another proposal here, so it is locked). Therefore,
        // the node's sibling will never ack, so the sibling's echoed proposal will never
        // be committed. I think that also this proposal will never be committed as well.
        self.set_log(index, ProposalSlot::Proposal(proposal));

        Ok(())
    }

    fn process_echo(
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

    fn try_ack(&mut self, index: usize, proposal: Proposal) -> Option<(NodeId, MessageKind)> {
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

    fn process_lock_reply(&mut self, from: NodeId, locked_state: LockedState) {
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

struct Croissus {
    network: VirtualNetwork<MessageKind>,
    state: Arc<Mutex<CroissusState>>,
    timeout: Duration,
    cancellation_token: CancellationToken,
}

impl Croissus {
    pub fn new(
        network: VirtualNetwork<MessageKind>,
        nodes: Vec<NodeId>,
        rtt: Duration,
        receive_channel: Receiver<Packet<MessageKind>>,
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
        info!("Node {} shutting down", self.network.node);
        self.cancellation_token.cancel()
    }

    pub async fn propose(&self, command: Command) -> (usize, CroissusResult) {
        let (index, result) = self.try_commit(command.clone()).await;
        (
            index,
            match result {
                Ok(_) => {
                    println!("COMMITTED {:?}", command);
                    CroissusResult::Committed(command)
                }
                Err(_) => {
                    let adopted_command = self.get_safe_value().await;
                    println!("ADOPTED {:?}", adopted_command);
                    CroissusResult::Adopted(self.get_safe_value().await)
                }
            },
        )
    }

    async fn try_commit(&self, command: Command) -> (usize, Result<()>) {
        let (proposal, index, finished_propose) = {
            let mut state = self.state.lock().await;
            // Don't have to check if the slot is locked, since the propose will
            // automatically pick the next unlocked slot
            state.propose(command.clone())
        };

        let timeout = tokio::time::sleep(self.timeout);

        self.diffuse_proposal(index, proposal).await;
        select! {
            _ = timeout => {
                debug!("Node {} ack timeout elapsed", self.network.node);
            }
            _ = finished_propose => {}
        }

        {
            let state = self.state.lock().await;
            if state
                .log
                .get(state.current_index)
                .unwrap()
                .as_ref()
                .unwrap()
                .done
            {
                return (index, Ok(()));
            }
        }

        (index, Err(anyhow!("Node couldn't commit, it must adopt")))
    }

    async fn diffuse_proposal(&self, index: usize, proposal: Proposal) {
        for destination in &proposal.flow.diffuse_to[&self.network.node] {
            debug!(
                "Node {} sending diffuse {:?} to {}",
                self.network.node, proposal, destination
            );
            self.network
                .send(
                    *destination,
                    MessageKind::Diffuse(DiffuseMessage {
                        index,
                        proposal: proposal.clone(),
                    }),
                )
                .await
                .unwrap()
        }
    }

    async fn get_safe_value(&self) -> Option<Command> {
        let (index, lock_phase_finished) = {
            let mut state = self.state.lock().await;
            state.go_to_lock_phase()
        };

        self.network
            .broadcast(MessageKind::Lock(LockMessage { index }))
            .await
            .unwrap();

        let result = lock_phase_finished.await;
        if let Err(error) = result {
            error!("Node {} failed to finish lock phase, sender part of channel - finish_lock_phase dropped. Error: {}", self.network.node, error);
            panic!("{error}");
        }
        result.unwrap()
    }

    async fn receive_loop(&self, mut recv_channel: Receiver<Packet<MessageKind>>) {
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
                        MessageKind::Diffuse(diffuse) => self.process_diffuse(packet.from, diffuse).await,
                        MessageKind::Echo(echo) => self.process_echo(packet.from, echo).await,
                        MessageKind::Ack(ack) => self.process_ack(packet.from, ack).await,
                        MessageKind::Lock(lock) => self.process_lock(packet.from, lock).await,
                        MessageKind::LockReply(lock_reply) => self.process_lock_reply(packet.from, lock_reply).await
                    };
                }
                _ = self.cancellation_token.cancelled() => {
                    info!("Terminated receive loop");
                    return;
                }
            }
        }
    }

    async fn process_diffuse(&self, from: NodeId, diffuse: DiffuseMessage) {
        debug!(
            "Node {} received diffuse {:?} from {}",
            self.network.node, diffuse, from
        );
        let mut state = self.state.lock().await;

        let diffuse_result = state.process_diffuse(diffuse.index, diffuse.proposal.clone());
        if let Err(error) = diffuse_result {
            debug!("{error}");
            return; // TODO: Return negative ack
        }

        for sibling in &diffuse.proposal.flow.echo_to[&self.network.node] {
            debug!(
                "Node {} sending echo {:?} to sibling {}",
                self.network.node, diffuse.proposal, sibling
            );
            self.network
                .send(
                    *sibling,
                    MessageKind::Echo(EchoMessage {
                        index: diffuse.index,
                        proposal: diffuse.proposal.clone(),
                    }),
                )
                .await
                .unwrap();
        }

        self.diffuse_proposal(diffuse.index, diffuse.proposal.clone())
            .await;

        if let Some((destination, message)) = state.try_ack(diffuse.index, diffuse.proposal) {
            debug!("Node {} sending ack to {}", self.network.node, destination);
            self.network.send(destination, message).await.unwrap()
        }
    }

    async fn process_echo(&self, from: NodeId, echo: EchoMessage) {
        debug!(
            "Node {} received echo {:?} from {}",
            self.network.node, echo, from
        );
        let mut state = self.state.lock().await;

        // TODO: Is it possible for 2 proposal's from different nodes to have the same echo?
        if let Some((destination, message)) = state.process_echo(from, echo.index, echo.proposal) {
            debug!("Node {} sending ack to {}", self.network.node, destination);
            self.network.send(destination, message).await.unwrap()
        }
    }

    async fn process_ack(&self, from: NodeId, ack: AckMessage) {
        debug!(
            "Node {} received ack {:?} from {}",
            self.network.node, ack, from
        );
        let mut state = self.state.lock().await;
        if ack.index >= state.log.len() {
            // TODO: Most likely not needed, since an ack has to come after we set proposal (as a response to diffuse)
            state.set_log(ack.index, ProposalSlot::None);
        }

        state.log[ack.index].as_mut().unwrap().acks.insert(from);

        if self.network.node == ack.proposal.proposer {
            // If the ack was delayed, ignore it
            if state.current_index != ack.index {
                return;
            }

            if state.can_ack(ack.index) {
                state.log[ack.index].as_mut().unwrap().done = true;
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
        } else if let Some((destination, message)) = state.try_ack(ack.index, ack.proposal) {
            debug!("Node {} sending ack to {}", self.network.node, destination);
            self.network.send(destination, message).await.unwrap()
        }
    }

    async fn process_lock(&self, from: NodeId, lock: LockMessage) {
        debug!(
            "Node {} received lock {:?} from {}",
            self.network.node, lock, from
        );
        let mut state = self.state.lock().await;
        let locked_state = state.lock(lock.index);

        debug!(
            "Node {} sending lock reply {:?} to {}",
            self.network.node, locked_state, from
        );
        self.network
            .send(
                from,
                MessageKind::LockReply(LockReplyMessage {
                    index: lock.index,
                    locked_state,
                }),
            )
            .await
            .unwrap()
    }

    async fn process_lock_reply(&self, from: NodeId, lock_reply: LockReplyMessage) {
        debug!(
            "Node {} received lock reply {:?} from {}",
            self.network.node, lock_reply, from
        );
        let mut state = self.state.lock().await;
        if state.current_index != lock_reply.index {
            return;
        }
        state.process_lock_reply(from, lock_reply.locked_state);
    }

    pub async fn get_log(&self) -> Vec<Option<LockedState>> {
        self.state.lock().await.log.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::command::{Command, CommandKind, WriteCommand};
    use crate::core::create_channel_network;
    use crate::croissus::{Croissus, CroissusResult};
    use futures::future::TryJoinAll;
    use std::collections::HashMap;
    use std::env::set_var;
    use std::sync::Once;
    use std::time::Duration;

    static INIT_LOGGER: Once = Once::new();
    fn init_logger() {
        INIT_LOGGER.call_once(|| {
            unsafe {
                set_var("RUST_LOG", "TRACE");
            }
            pretty_env_logger::init_timed();
        });
    }

    #[tokio::test]
    async fn test_croissus_synchronous() {
        init_logger();
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
                Duration::from_millis(100),
                receive_channel,
            );
            croissuses.insert(node.clone(), croissus);
            spawned_tasks.push(join_handle);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        let mut adopt_commit_log = HashMap::new();
        for proposer in &nodes {
            let (index, croissus_result) = croissuses[proposer]
                .propose(Command {
                    id: *proposer as u64,
                    client_id: *proposer as u64,
                    command_kind: CommandKind::Write(WriteCommand {
                        key: *proposer as u64,
                        value: 10 + *proposer as u32,
                    }),
                })
                .await;
            adopt_commit_log
                .entry(index)
                .or_insert(Vec::new())
                .push(croissus_result);

            for other in nodes.iter().filter(|node| *node != proposer) {
                let (index, croissus_result) = croissuses[other]
                    .propose(Command {
                        id: *other as u64,
                        client_id: *other as u64,
                        command_kind: CommandKind::Write(WriteCommand {
                            key: *other as u64,
                            value: 10 + *other as u32,
                        }),
                    })
                    .await;
                adopt_commit_log
                    .entry(index)
                    .or_insert(Vec::new())
                    .push(croissus_result);
            }
        }

        assert!(adopt_commit_log.values().all(|results| is_valid(results)));

        for (_, croissus) in croissuses.drain() {
            croissus.quit();
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
        spawned_tasks
            .into_iter()
            .collect::<TryJoinAll<_>>()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_croissus_concurrent() {
        init_logger();
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
                Duration::from_millis(100),
                receive_channel,
            );
            croissuses.insert(node.clone(), croissus);
            spawned_tasks.push(join_handle);
        }

        let c4 = croissuses.remove(&2).unwrap();
        let c3 = croissuses.remove(&5).unwrap();
        let c3_clone = c3.clone();
        let c4_clone = c4.clone();
        let f1 = tokio::spawn(async move {
            c4_clone
                .propose(Command {
                    id: 1,
                    client_id: 69,
                    command_kind: CommandKind::Write(WriteCommand { key: 1, value: 10 }),
                })
                .await
        });
        let f2 = tokio::spawn(async move {
            c3_clone
                .propose(Command {
                    id: 2,
                    client_id: 70,
                    command_kind: CommandKind::Write(WriteCommand {
                        key: 12,
                        value: 102,
                    }),
                })
                .await
        });

        let result1 = f1.await;
        assert!(result1.is_ok());
        let (index1, result1) = result1.unwrap();

        let result2 = f2.await;
        assert!(result2.is_ok());
        let (index2, result2) = result2.unwrap();
        if index1 == index2 {
            assert!(is_valid(&vec![result1, result2]));
        } else {
            assert!(result1.is_committed());
            assert!(result2.is_committed());
        }

        c3.quit();
        c4.quit();
        for (_, croissus) in croissuses.drain() {
            croissus.quit();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        spawned_tasks
            .into_iter()
            .collect::<TryJoinAll<_>>()
            .await
            .unwrap();
    }

    fn is_valid(results: &Vec<CroissusResult>) -> bool {
        let committed = results
            .iter()
            .filter_map(|result| match result {
                CroissusResult::Committed(command) => Some(command),
                CroissusResult::Adopted(_) => None,
            })
            .collect::<Vec<_>>();

        if committed.len() > 1 {
            return false;
        }

        if committed.len() == 0 {
            return true;
        }

        let committed = committed[0];
        results.iter().all(|result| match result {
            CroissusResult::Committed(command) => command == committed,
            CroissusResult::Adopted(command) => command
                .as_ref()
                .map(|command| command == committed)
                .is_some(),
        })
    }
}
