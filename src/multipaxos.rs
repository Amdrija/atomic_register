use crate::core::{NodeId, Packet, VirtualNetwork};
use anyhow::{anyhow, Result};
use futures::future::JoinAll;
use log::{debug, info};
use rkyv::{Archive, Deserialize, Serialize};
use std::cmp::min;
use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::thread::sleep;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, Mutex};
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
    leader: NodeId,
    heard_from: Vec<NodeId>,
    log: Vec<Option<LogEntry>>,
    min_proposal: ProposalNumber,
    max_round: u64,
    next_index: usize,
    prepared: bool,
}

impl MultipaxosState {
    fn new(node: NodeId) -> Self {
        let zero_proposal = ProposalNumber {
            round: 0,
            node,
        };

        Self {
            node: node,
            leader: node,
            heard_from: vec![],
            log: vec![Some(LogEntry {
                proposal_number: zero_proposal.clone(),
                command: Command::NoOp,
            })],
            min_proposal: zero_proposal.clone(),
            max_round: zero_proposal.round,
            next_index: 1,
            prepared: false
        }
    }

    fn elect_leader(&mut self) {
        debug!("Node {} heard from: {:?}", self.node, self.heard_from);
        let highest_heard = self.heard_from.iter().max().unwrap_or(&self.node);
        self.leader = *highest_heard;
        debug!("Node {} elected {} for leader", self.node, self.leader);
        self.heard_from.clear();
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
        let multipaxos = Arc::new(Multipaxos {
            network: virtual_network,
            heartbeat_delay,
            state: Arc::new(Mutex::new(MultipaxosState::new(node))),
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
    // because the prepare phase will have to run for all
    // and then they will potentially live-lock each other
    pub async fn issue_command(&self, command: Command) -> Result<()> {
        // TODO: Implement
        Ok(())
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
        // TODO: Implement
    }

    async fn process_reject_prepare(&self, reject_prepare_message: RejectPrepareMessage) {
        // TODO: Implement
    }

    async fn process_promise(&self, promise_message: PromiseMessage) {
        // TODO: Implement
    }

    async fn process_propose(&self, propose_message: ProposeMessage, from: NodeId) {
        // TODO: Implement
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
    use log::{debug, log_enabled, Level};
    use std::collections::HashMap;
    use std::env::set_var;
    use std::time::Duration;

    #[tokio::test]
    async fn test_multipaxos() {
        unsafe {
            set_var("RUST_LOG", "DEBUG");
        }
        pretty_env_logger::init_timed();
        let nodes = vec![0, 1, 2, 3, 4];
        let (mut virtual_networks, mut receivers) = create_channel_network(nodes.clone());

        let mut multipaxoses = HashMap::new();
        let mut spawned_tasks = Vec::new();
        for node in &nodes {
            let network = virtual_networks.remove(node).unwrap();
            let receive_channel = receivers.remove(node).unwrap();
            let (multipaxos, background_tasks) =
                Multipaxos::new(network, Duration::from_millis(100), receive_channel);
            multipaxoses.insert(node, multipaxos);
            spawned_tasks.push(background_tasks);
        }

        tokio::time::sleep(Duration::from_millis(320)).await;

        let result = multipaxoses[&0].issue_command(Command::Write(WriteCommand{
            key: 1,
            value: 10
        })).await;

        assert!(result.is_ok());

        for node in &nodes {
            multipaxoses[node].quit();
        }

        spawned_tasks.into_iter().collect::<JoinAll<_>>().await;

        assert!(false);
    }
}
