use crate::core::{NodeId, Packet, VirtualNetwork};
use anyhow::{anyhow, bail, Context, Result};
use futures::future::{ok, JoinAll};
use log::{debug, error, info};
use rkyv::{Archive, Deserialize, Serialize};
use std::cmp::min;
use std::future::Future;
use std::ops::Deref;
use std::os::macos::raw::stat;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::thread::sleep;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio::{select, time};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize, Archive, PartialEq, Eq, PartialOrd, Ord)]
struct ProposalNumber {
    round: u64,
    node: NodeId,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive, PartialEq, Eq)]
struct WriteCommand {
    key: u64,
    value: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive, PartialEq, Eq)]
enum Command {
    NoOp,
    Write(WriteCommand),
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
struct PrepareMessage {
    proposal_number: ProposalNumber,
    index: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
struct PromiseMessage {
    proposal_number: ProposalNumber,
    accepted: Option<LogEntry>,
    no_more_accepted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
struct RejectPrepareMessage {
    proposal_number: ProposalNumber
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
struct ProposeMessage {
    proposal_number: ProposalNumber,
    index: usize,
    command: Command,
    first_unchosen_index: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
struct AcceptMessage {
    proposal_number: ProposalNumber,
    first_unchosen_index: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
struct SuccessMessage {
    index: usize,
    command: Command,
}

// TODO: Check if we actually need to identify this message
#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
struct SuccessResponseMessage {
    first_unchosen_index: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
enum Message {
    Heartbeat,
    Prepare(PrepareMessage),
    Promise(PromiseMessage),
    RejectPrepare(RejectPrepareMessage),
    Propose(ProposeMessage),
    Accept(AcceptMessage),
    Success(SuccessMessage),
    SuccessResponse(SuccessResponseMessage),
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
struct LogEntry {
    proposal_number: ProposalNumber,
    command: Command,
}

const INFINITE_PROPOSAL: ProposalNumber = ProposalNumber {
    round: u64::MAX,
    node: NodeId::MAX,
};

struct MultipaxosState {
    node: NodeId,
    majority_threshold: usize,
    leader: NodeId,
    heard_from: Vec<NodeId>,
    log: Vec<Option<LogEntry>>,
    min_promised_proposal: ProposalNumber,
    max_round: u64,
    next_index: usize,
    prepared: bool,
    current_proposal: ProposalNumber,
    promises: Vec<PromiseMessage>,
    finish_prepare: Option<oneshot::Sender<Option<LogEntry>>>,
    accept_acks: NodeId,
    finish_accept: Option<oneshot::Sender<()>>
}

impl MultipaxosState {
    fn new(node: NodeId, node_count: usize) -> Self {
        let zero_proposal = ProposalNumber {
            round: 0,
            node,
        };

        Self {
            node,
            majority_threshold: node_count / 2 + 1,
            leader: node,
            heard_from: vec![],
            log: vec![Some(LogEntry {
                proposal_number: INFINITE_PROPOSAL.clone(),
                command: Command::NoOp,
            })],
            min_promised_proposal: zero_proposal.clone(),
            max_round: zero_proposal.round,
            next_index: 1,
            prepared: false,
            current_proposal: zero_proposal.clone(),
            promises: vec![],
            finish_prepare: None,
            accept_acks: 0,
            finish_accept: None
        }
    }

    fn am_i_leader(&self) -> bool {
        self.leader == self.node
    }

    fn elect_leader(&mut self) {
        debug!("Node {} heard from: {:?}", self.node, self.heard_from);
        let highest_heard = self.heard_from.iter().max().unwrap_or(&self.node);
        self.leader = *highest_heard;
        debug!("Node {} elected {} for leader", self.node, self.leader);
        self.heard_from.clear();
    }

    fn go_to_prepare_state(&mut self) -> (PrepareMessage, oneshot::Receiver<Option<LogEntry>>) {
        let index = self.first_unchosen_index();
        self.next_index = index + 1;

        self.max_round += 1;
        let proposal_number = ProposalNumber {
            round: self.max_round,
            node: self.node
        };
        self.current_proposal = proposal_number.clone();

        self.promises.clear();

        let (send, recv) = oneshot::channel();
        self.finish_prepare.replace(send);

        (PrepareMessage {
            proposal_number,
            index,
        }, recv)
    }

    fn skip_prepare_state(&mut self) -> usize {
        self.next_index += 1;
        self.next_index - 1
    }

    fn promise(&mut self, prepare: &PrepareMessage) -> Option<LogEntry> {
        self.min_promised_proposal = prepare.proposal_number.clone();

        self.log.get(prepare.index).cloned().unwrap_or(None)
    }

    fn no_more_accepted(&self, index: usize) -> bool {
        if index + 1 >= self.log.len() {
            return true;
        }

        self.log[index + 1..].iter().all(|entry| entry.is_none())
    }

    fn fail_prepare_phase(&mut self) {
        if let Some(finish_prepare) = self.finish_prepare.take() {
            self.promises.clear();
            drop(finish_prepare); // Basically, this will close the sending part of the channel and the recv part will error
        }
    }

    fn process_promise(&mut self, promise_message: PromiseMessage) {
        self.promises.push(promise_message);
        if self.promises.len() == self.majority_threshold {
            if let Some(finish_prepare) = self.finish_prepare.take() {
                let highest_accepted = self.promises.iter().filter_map(|promise| promise.accepted.clone()).max_by_key(|entry| entry.proposal_number.clone());
                match finish_prepare.send(highest_accepted) {
                    Ok(_) => {
                        self.prepared = self.promises.iter().all(|promise| promise.no_more_accepted);
                    }
                    Err(_) => {
                        info!("Node {} failed to finish prepare phase because nobody listening in the issue command function - prepare already finished", self.node);
                    }
                }
            }
        }
    }

    fn go_to_accept_phase(&mut self, index: usize, command: Command) -> (ProposeMessage, oneshot::Receiver<()>) {
        let (accept_send, accept_recv) = oneshot::channel();
        self.finish_accept.replace(accept_send);
        self.accept_acks = 0;

        (ProposeMessage {
            proposal_number: self.current_proposal.clone(),
            index,
            command,
            first_unchosen_index: self.first_unchosen_index(),
        }, accept_recv)
    }

    fn set_log(
        &mut self,
        index: usize,
        proposal_number: ProposalNumber,
        command: Command,
    ) {
        if index >= self.log.len() {
            self.log.reserve(index - self.log.len() + 1);
        }
        self.log[index] = Some(LogEntry {
            proposal_number,
            command,
        });
    }

    fn first_unchosen_index(&self) -> usize {
        self.log.iter()
            .enumerate()
            .find(|(index, entry)| {
                entry.is_some() && entry.as_ref().unwrap().proposal_number < INFINITE_PROPOSAL
            })
            .map(|(index, _)| index)
            .unwrap_or(self.log.len())
    }
}

struct Multipaxos {
    network: VirtualNetwork<Message>,
    heartbeat_delay: Duration,
    state: Arc<Mutex<MultipaxosState>>,
    cancellation_token: CancellationToken
}

impl Multipaxos {
    pub fn new(
        virtual_network: VirtualNetwork<Message>,
        heartbeat_delay: Duration,
        receive_channel: Receiver<Packet<Message>>,
    ) -> (Arc<Self>, impl Future) {
        let node = virtual_network.node;
        let nodes = virtual_network.len();

        let multipaxos = Arc::new(Multipaxos {
            network: virtual_network,
            heartbeat_delay,
            state: Arc::new(Mutex::new(MultipaxosState::new(node, nodes))),
            cancellation_token: CancellationToken::new()
        });

        let multipaxos_clone = multipaxos.clone();
        let heartbeat_procssing_handle = tokio::spawn(async move {
            multipaxos_clone.heartbeat_processing_loop().await;
        });

        let multipaxos_clone = multipaxos.clone();
        let receive_handle = tokio::spawn(async move {
            multipaxos_clone.receive_loop(receive_channel).await;
        });

        let multipaxos_clone = multipaxos.clone();
        let heartbeat_handle = tokio::spawn(async move {
            multipaxos_clone.heartbeat_loop().await;
        });

        let cleanup_task = async move {
            heartbeat_handle.await.unwrap();
            heartbeat_procssing_handle.await.unwrap();
            receive_handle.await.unwrap();
        };

        (multipaxos, cleanup_task)
    }

    // No point in issuing multiple concurrent commands
    // because the prepare phase will have to run for all,
    // and then they will potentially live-lock each other
    pub async fn issue_command(&self, command: Command) -> Result<()> {
        let prepared =  {
            let mut state = self.state.lock().await;

            if !state.am_i_leader() {
                return bail!("Current node not leader, leader={}", state.leader);
            }

            state.prepared
        };

        let (index, command_to_propose) = if !prepared {
            let (prepare_message, prepare_finished) = {
                let mut state = self.state.lock().await;
                state.go_to_prepare_state()
            };
            let index = prepare_message.index;

            self.network.broadcast(Message::Prepare(prepare_message)).await?;

            // If this await fails, that means that the send side was dropped
            // This is only possible if the prepare message was rejected by a node
            let accepted_entry = prepare_finished.await.with_context(|| "Prepare rejected from a node, try again")?;

            info!("Node {} prepare successful", self.network.node);

            (index, accepted_entry.map(|entry| entry.command).unwrap_or(command.clone()))
        } else {
            let mut state = self.state.lock().await;
            info!("Node {} skipping prepare state", self.network.node);
            (state.skip_prepare_state(), command.clone())
        };

        return Ok(());

        // TODO: Implement accept phase
        // let (propose_message, accept_finished) = state.go_to_accept_phase(index, command_to_propose.clone());
        // self.network.broadcast(Message::Propose(propose_message)).await?;
        //
        // accept_finished.await.with_context(|| "Proposal rejected from a node, try again")?;
        //
        // if command_to_propose != command {
        //     return bail!("The proposal slot {} is taken, try again", index);
        // }
        //
        // Ok(())
    }

    pub fn quit(&self) {
        self.cancellation_token.cancel();
    }

    async fn receive_loop(&self, mut recv_channel: Receiver<Packet<Message>>) {
        loop {
            select! {
                result = recv_channel.recv() => {
                    if result.is_none() {
                        info!("Receive loop receiving channel closed");
                        return;
                    }

                    let packet = result.unwrap();
                    match packet.data {
                        Message::Heartbeat => {self.process_heartbeat(packet.from).await}
                        Message::Prepare(prepare) => {self.process_prepare(prepare, packet.from).await}
                        Message::RejectPrepare(reject_prepare) => {self.process_reject_prepare(reject_prepare).await}
                        Message::Promise(promise) => {self.process_promise(promise).await}
                        Message::Propose(propose) => {self.process_propose(propose, packet.from).await}
                        Message::Accept(accept) => {self.process_accept(accept, packet.from).await}
                        Message::Success(success) => {self.process_success(success, packet.from).await}
                        Message::SuccessResponse(success_response) => {self.process_success_ressponse(success_response, packet.from).await}
                    };
                }
                _ = self.cancellation_token.cancelled() => {
                    info!("Terminated receive loop");
                    return;
                }
            }
        }
    }

    async fn heartbeat_loop(&self) {
        let mut interval = tokio::time::interval(self.heartbeat_delay);
        loop {
            select! {
                _ = interval.tick() => {
                    debug!("Node {} broadcasting heartbeat", self.network.node);
                    self.network.broadcast(Message::Heartbeat).await.unwrap();
                }
                _ = self.cancellation_token.cancelled() => {
                    info!("Terminated receive loop");
                    return;
                }
            }
        }
    }

    async fn heartbeat_processing_loop(&self) {
        let mut interval = tokio::time::interval(self.heartbeat_delay * 2);
        loop {
            select! {
                _ = interval.tick() => {
                    let mut state = self.state.lock().await;
                    state.elect_leader();
                }
                _ = self.cancellation_token.cancelled() => {
                    info!("Terminated heartbeat processing loop");
                    return;
                }
            }
        }
    }

    async fn process_heartbeat(&self, from: NodeId) {
        //TODO: Implement
        debug!("Node {} received heartbeat from {from}", self.network.node);
        let mut state = self.state.lock().await;
        state.heard_from.push(from);
    }

    async fn process_prepare(&self, prepare_message: PrepareMessage, from: NodeId) {
        debug!("Node {} processing prepare message {:?} from {}", self.network.node, prepare_message, from);
        let mut state = self.state.lock().await;
        if prepare_message.proposal_number >= state.min_promised_proposal {
            let accepted = state.promise(&prepare_message);
            let promise_message = PromiseMessage {
                proposal_number: prepare_message.proposal_number,
                accepted,
                no_more_accepted: state.no_more_accepted(prepare_message.index),
            };

            debug!("Node {} sending promise messagee {:?} to {}", self.network.node, promise_message, from);
            self.network.send(from, Message::Promise(promise_message)).await.unwrap();
        } else {
            let reject_message = RejectPrepareMessage { proposal_number: state.min_promised_proposal.clone() };

            debug!("Node {} rejecting prepare message {:?} from {}", self.network.node, prepare_message, from);
            self.network.send(from, Message::RejectPrepare(reject_message)).await.unwrap();
        }
    }

    async fn process_reject_prepare(&self, reject_prepare_message: RejectPrepareMessage) {
        let mut state = self.state.lock().await;
        if state.current_proposal < reject_prepare_message.proposal_number {
            state.fail_prepare_phase();
        }
    }

    async fn process_promise(&self, promise_message: PromiseMessage) {
        debug!("Node {} received promise message {:?}", self.network.node, promise_message);
        let mut state = self.state.lock().await;
        if promise_message.proposal_number == state.current_proposal {
            state.process_promise(promise_message);
        }
    }

    async fn process_propose(&self, propose_message: ProposeMessage, from: NodeId) {

    }

    async fn process_accept(&self, accept_message: AcceptMessage, from: NodeId) {
        // TODO: Implement
    }

    async fn process_success(&self, success_message: SuccessMessage, from: NodeId) {
        // TODO: Implement
    }

    async fn process_success_ressponse(
        &self,
        success_response_message: SuccessResponseMessage,
        from: NodeId,
    ) {
        // TODO: Implement
    }
}

#[cfg(test)]
mod test {
    use crate::core::create_channel_network;
    use crate::multipaxos::{Command, Multipaxos, WriteCommand};
    use futures::future::JoinAll;
    use log::{debug, info, log_enabled, Level};
    use std::collections::HashMap;
    use std::env::set_var;
    use std::time::Duration;

    #[tokio::test]
    async fn test_multipaxos() {
        unsafe {
            set_var("RUST_LOG", "DEBUG");
        }
        pretty_env_logger::init_timed();
        let nodes = vec![0, 1, 2];
        let (mut virtual_networks, mut receivers) = create_channel_network(nodes.clone());

        let mut multipaxoses = HashMap::new();
        let mut spawned_tasks = Vec::new();
        for node in &nodes {
            let network = virtual_networks.remove(node).unwrap();
            let receive_channel = receivers.remove(node).unwrap();
            let (multipaxos, background_tasks) =
                Multipaxos::new(network, Duration::from_millis(1000), receive_channel);
            multipaxoses.insert(node, multipaxos);
            spawned_tasks.push(background_tasks);
        }

        tokio::time::sleep(Duration::from_millis(2100)).await;

        let result = multipaxoses[&2].issue_command(Command::Write(WriteCommand{
            key: 1,
            value: 10
        })).await;

        info!("*********** Result {:?}", result);

        assert!(result.is_ok());

        for node in &nodes {
            multipaxoses[node].quit();
        }

        spawned_tasks.into_iter().collect::<JoinAll<_>>().await;

        assert!(false);
    }
}