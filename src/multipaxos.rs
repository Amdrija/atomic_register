use crate::core::{NodeId, Packet, VirtualNetwork};
use anyhow::{anyhow, bail, Context, Result};
use futures::future::{ok, JoinAll};
use log::{debug, error, info};
use rkyv::{Archive, Deserialize, Serialize};
use std::cmp::min;
use std::future::Future;
use std::ops::Deref;
use std::os::macos::raw::stat;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
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
enum CommandKind {
    NoOp,
    Write(WriteCommand),
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive, PartialEq, Eq)]
struct Command {
    id: u64,
    client_id: u64,
    command_kind: CommandKind,
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
    proposal_number: ProposalNumber,
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
struct ReadMessage {
    id: u64,
    chosen_sequence_end: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
struct ReadResponseMessage {
    id: u64,
    chosen_sequence_end: usize,
    missing_committed_entries: Vec<LogEntry>,
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
    Read(ReadMessage),
    ReadResponse(ReadResponseMessage),
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive, PartialEq, Eq)]
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
    accept_acks: usize,
    finish_accept: Option<oneshot::Sender<()>>,
    next_read_id: u64,
    current_read_id: u64,
    read_responses: Vec<Vec<LogEntry>>,
    finish_read: Option<oneshot::Sender<bool>>,
}

impl MultipaxosState {
    fn new(node: NodeId, node_count: usize) -> Self {
        let zero_proposal = ProposalNumber { round: 0, node };

        Self {
            node,
            majority_threshold: node_count / 2 + 1,
            leader: node,
            heard_from: vec![],
            log: vec![Some(LogEntry {
                proposal_number: INFINITE_PROPOSAL.clone(),
                command: Command {
                    id: 0,
                    client_id: 0,
                    command_kind: CommandKind::NoOp,
                },
            })],
            min_promised_proposal: zero_proposal.clone(),
            max_round: zero_proposal.round,
            next_index: 1,
            prepared: false,
            current_proposal: zero_proposal.clone(),
            promises: vec![],
            finish_prepare: None,
            accept_acks: 0,
            finish_accept: None,
            next_read_id: 1,
            current_read_id: 0,
            read_responses: vec![],
            finish_read: None,
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
            node: self.node,
        };
        self.current_proposal = proposal_number.clone();

        self.promises.clear();

        let (send, recv) = oneshot::channel();
        self.finish_prepare.replace(send);

        (
            PrepareMessage {
                proposal_number,
                index,
            },
            recv,
        )
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
                let highest_accepted = self
                    .promises
                    .iter()
                    .filter_map(|promise| promise.accepted.clone())
                    .max_by_key(|entry| entry.proposal_number.clone());
                match finish_prepare.send(highest_accepted) {
                    Ok(_) => {
                        self.prepared =
                            self.promises.iter().all(|promise| promise.no_more_accepted);
                    }
                    Err(_) => {
                        info!("Node {} failed to finish prepare phase because nobody listening in the issue command function - prepare already finished", self.node);
                    }
                }
            }
        }
    }

    fn go_to_accept_phase(
        &mut self,
        index: usize,
        command: Command,
    ) -> (ProposeMessage, oneshot::Receiver<()>) {
        let (accept_send, accept_recv) = oneshot::channel();
        self.finish_accept.replace(accept_send);
        self.accept_acks = 0;

        (
            ProposeMessage {
                proposal_number: self.current_proposal.clone(),
                index,
                command,
                first_unchosen_index: self.first_unchosen_index(),
            },
            accept_recv,
        )
    }

    fn accept_or_reject_proposal(&mut self, propose_message: ProposeMessage) -> AcceptMessage {
        if propose_message.proposal_number >= self.min_promised_proposal {
            self.min_promised_proposal = propose_message.proposal_number.clone();
            self.set_log(
                propose_message.index,
                propose_message.proposal_number.clone(),
                propose_message.command,
            );
            self.set_chosen_up_to(propose_message.index, &propose_message.proposal_number);
        }

        AcceptMessage {
            proposal_number: self.min_promised_proposal.clone(),
            first_unchosen_index: self.first_unchosen_index(),
        }
    }

    fn process_rejected_proposal(&mut self, accepted_round: u64) {
        self.max_round = accepted_round;
        self.prepared = false;
        if let Some(finish_accept) = self.finish_accept.take() {
            self.accept_acks = 0;
            drop(finish_accept); // This will error the receiving side
        }
    }

    fn go_to_read_phase(
        &mut self,
        chosen_sequence_end: usize,
    ) -> (ReadMessage, oneshot::Receiver<bool>) {
        let read_message = ReadMessage {
            id: self.next_read_id,
            chosen_sequence_end,
        };
        self.next_read_id += 1;

        self.read_responses.clear();
        self.current_read_id = read_message.id;

        let (finish_read_send, finish_read_recv) = oneshot::channel();
        self.finish_read.replace(finish_read_send);

        (read_message, finish_read_recv)
    }

    fn process_read_response(&mut self, read_response_message: ReadResponseMessage) {
        if self.current_read_id == read_response_message.id {
            self.read_responses
                .push(read_response_message.missing_committed_entries);

            if self.read_responses.len() == self.majority_threshold {
                let all_equal = self.all_responses_equal();

                if all_equal {
                    self.set_chosen_entries_from(
                        &self.read_responses[0].clone(),
                        read_response_message.chosen_sequence_end + 1,
                    )
                }

                self.finish_read.take().unwrap().send(all_equal).unwrap()
            }
        }
    }

    fn all_responses_equal(&self) -> bool {
        let first = &self.read_responses[0];

        self.read_responses.iter().all(|entry| entry == first)
    }

    fn set_chosen_entries_from(&mut self, chosen_entries: &Vec<LogEntry>, from: usize) {
        let mut current = from;
        for entry in chosen_entries {
            self.set_log(
                current,
                entry.proposal_number.clone(),
                entry.command.clone(),
            );
            current += 1;
        }
    }

    fn get_chosen_entry(&self, index: usize) -> Option<LogEntry> {
        self.log
            .get(index)
            .map(|entry| {
                entry.clone().map(|entry| {
                    if entry.proposal_number == INFINITE_PROPOSAL {
                        Some(entry)
                    } else {
                        None
                    }
                })
            })
            .flatten()
            .flatten()
    }

    fn set_log(&mut self, index: usize, proposal_number: ProposalNumber, command: Command) {
        if index >= self.log.len() {
            self.log.resize(index + 1, None);
        }
        self.log[index] = Some(LogEntry {
            proposal_number,
            command,
        });
    }

    fn set_chosen_up_to(&mut self, index: usize, proposal_number: &ProposalNumber) {
        if index > self.log.len() {
            return;
        }

        self.log[..index].iter_mut().for_each(|entry| {
            if let Some(entry) = entry {
                if entry.proposal_number == *proposal_number {
                    entry.proposal_number = INFINITE_PROPOSAL.clone();
                }
            }
        });
    }

    fn first_unchosen_index(&self) -> usize {
        self.log
            .iter()
            .enumerate()
            .find(|(index, entry)| {
                entry.is_some() && entry.as_ref().unwrap().proposal_number < INFINITE_PROPOSAL
            })
            .map(|(index, _)| index)
            .unwrap_or(self.log.len())
    }

    fn chosen_sequence_end(&self) -> usize {
        for i in 0..self.log.len() - 1 {
            match &self.log[i] {
                None => return i,
                Some(entry) => {
                    if entry.proposal_number != INFINITE_PROPOSAL {
                        return i;
                    }
                }
            }
        }

        self.log.len() - 1
    }

    fn get_chosen_sequence_from(&self, index: usize) -> Vec<LogEntry> {
        let until = self.chosen_sequence_end();

        self.log
            .get(index..until + 1)
            .unwrap_or_default()
            .iter()
            .cloned()
            .map(|entry| entry.unwrap())
            .collect::<Vec<_>>()
    }
}

struct Multipaxos {
    network: VirtualNetwork<Message>,
    heartbeat_delay: Duration,
    state: Arc<Mutex<MultipaxosState>>,
    cancellation_token: CancellationToken,
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
            cancellation_token: CancellationToken::new(),
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
        let prepared = {
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

            self.network
                .broadcast(Message::Prepare(prepare_message))
                .await?;

            // If this await fails, that means that the send side was dropped
            // This is only possible if the prepare message was rejected by a node
            let accepted_entry = prepare_finished
                .await
                .with_context(|| "Prepare rejected from a node, try again")?;

            info!("Node {} prepare successful", self.network.node);

            (
                index,
                accepted_entry
                    .map(|entry| entry.command)
                    .unwrap_or(command.clone()),
            )
        } else {
            let mut state = self.state.lock().await;
            info!("Node {} skipping prepare state", self.network.node);
            (state.skip_prepare_state(), command.clone())
        };

        let (propose_message, accept_finished) = {
            let mut state = self.state.lock().await;
            state.go_to_accept_phase(index, command_to_propose.clone())
        };

        let (propose_message, accept_finished) = {
            let mut state = self.state.lock().await;
            state.go_to_accept_phase(index, command_to_propose.clone())
        };
        self.network
            .broadcast(Message::Propose(propose_message))
            .await?;

        accept_finished
            .await
            .with_context(|| "Proposal rejected from a node, try again")?;

        {
            let mut state = self.state.lock().await;
            state.set_log(index, INFINITE_PROPOSAL.clone(), command_to_propose.clone());
        }

        // Help everyone out so they know this command was decided at this index
        // It will not be possible to decide anything else at this index, since
        // a majority must have accepted the command and this index in order to
        // get here, so, there's a couple of possibilities:
        // 1. Somebody tries to prepare at this index, well, because a majority
        // accepted this value, at least 1 will return this accepted value to the
        // preparer, so the preparer will be force to propose this value
        // 2. Somebody does a prepare with a higher number, but nobody has accepted
        // anything at this index -> then, because of the prepare, it would not be possible
        // to get majority accepts
        // 3. Somebody does a prepare with a higher number, but another value was accepted,
        // so the new preparer has to propose a different value. Not possible, since that would
        // mean that 1 node has accepted a value with proposal number n', meaning that n' was prepared
        // by a majority. If n' is greater than the current proposal number, at least 1 node in the
        // majority would have to have rejected the proposal, since it promised a higher number.
        // If n' was smaller than current proposal, it would not have been able to be prepared, or
        // if it was prepared it would not have been accepted (because current prepared interrupted it)
        // or if the value was accepted, it must have then been proposed in the current proposal.
        let success_message = SuccessMessage {
            index,
            command: command.clone(),
        };
        self.network
            .broadcast(Message::Success(success_message))
            .await?;

        if command_to_propose != command {
            return bail!("The proposal slot {} is taken, try again", index);
        }

        Ok(())
    }

    // TODO: Probably want to read latest information instead
    // TODO: Update the state when this is called and everytime a subsequent entry gets chosen
    pub async fn read_log(&self, start: usize) -> Vec<LogEntry> {
        let chosen_sequence_end = {
            let mut state = self.state.lock().await;
            state.chosen_sequence_end()
        };

        loop {
            let (read_message, finish_read_recv) = {
                let mut state = self.state.lock().await;
                state.go_to_read_phase(chosen_sequence_end)
            };
            self.network
                .broadcast(Message::Read(read_message))
                .await
                .unwrap();

            let majority_equal = finish_read_recv.await.unwrap();
            if majority_equal {
                break;
            }
        }

        {
            let mut state = self.state.lock().await;
            state
                .log
                .get(start..state.chosen_sequence_end() + 1)
                .unwrap_or_default()
                .iter()
                .cloned()
                .map(|entry| entry.unwrap())
                .collect()
        }
    }

    pub async fn get_log(&self) -> Vec<Option<LogEntry>> {
        self.state.lock().await.log.clone()
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
                        Message::Prepare(prepare) => self.process_prepare(prepare, packet.from).await,
                        Message::RejectPrepare(reject_prepare) => self.process_reject_prepare(reject_prepare, packet.from).await,
                        Message::Promise(promise) => self.process_promise(promise, packet.from).await,
                        Message::Propose(propose) => self.process_propose(propose, packet.from).await,
                        Message::Accept(accept) => self.process_accept(accept, packet.from).await,
                        Message::Success(success) => self.process_success(success, packet.from).await,
                        Message::SuccessResponse(success_response) => self.process_success_response(success_response, packet.from).await,
                        Message::Read(read) => self.process_read(read, packet.from).await,
                        Message::ReadResponse(read_response) => self.process_read_response(read_response, packet.from).await,
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
        debug!("Node {} received heartbeat from {from}", self.network.node);
        let mut state = self.state.lock().await;
        state.heard_from.push(from);
    }

    async fn process_prepare(&self, prepare_message: PrepareMessage, from: NodeId) {
        debug!(
            "Node {} processing prepare message {:?} from {}",
            self.network.node, prepare_message, from
        );
        let mut state = self.state.lock().await;
        if prepare_message.proposal_number >= state.min_promised_proposal {
            let accepted = state.promise(&prepare_message);
            let promise_message = PromiseMessage {
                proposal_number: prepare_message.proposal_number,
                accepted,
                no_more_accepted: state.no_more_accepted(prepare_message.index),
            };

            self.network
                .send(from, Message::Promise(promise_message.clone()))
                .await
                .unwrap();
            debug!(
                "Node {} sent promise message {:?} to {}",
                self.network.node, promise_message, from
            );
        } else {
            let reject_message = RejectPrepareMessage {
                proposal_number: state.min_promised_proposal.clone(),
            };

            self.network
                .send(from, Message::RejectPrepare(reject_message))
                .await
                .unwrap();
            debug!(
                "Node {} rejected prepare message {:?} from {}",
                self.network.node, prepare_message, from
            );
        }
    }

    async fn process_reject_prepare(
        &self,
        reject_prepare_message: RejectPrepareMessage,
        from: NodeId,
    ) {
        debug!(
            "Node {} received reject prepare {:?} from {}",
            self.network.node, reject_prepare_message, from
        );
        let mut state = self.state.lock().await;
        if state.current_proposal < reject_prepare_message.proposal_number {
            state.fail_prepare_phase();
            debug!("Node {} failed prepare phase", self.network.node);
        }
    }

    async fn process_promise(&self, promise_message: PromiseMessage, from: NodeId) {
        debug!(
            "Node {} received promise message {:?} from {}",
            self.network.node, promise_message, from
        );
        let mut state = self.state.lock().await;
        if promise_message.proposal_number == state.current_proposal {
            state.process_promise(promise_message);
        }
    }

    async fn process_propose(&self, propose_message: ProposeMessage, from: NodeId) {
        debug!(
            "Node {} received propose message {:?} from {}",
            self.network.node, propose_message, from
        );
        let mut state = self.state.lock().await;
        let accept_message = state.accept_or_reject_proposal(propose_message);

        self.network
            .send(from, Message::Accept(accept_message.clone()))
            .await
            .unwrap();
        debug!(
            "Node {} sent accept {:?} to {}",
            self.network.node, accept_message, from
        );
    }

    async fn process_accept(&self, accept_message: AcceptMessage, from: NodeId) {
        debug!(
            "Node {} received accept message {:?} from {}",
            self.network.node, accept_message, from
        );
        let mut state = self.state.lock().await;
        if accept_message.proposal_number > state.current_proposal {
            state.process_rejected_proposal(accept_message.proposal_number.round);
        } else if accept_message.proposal_number == state.current_proposal {
            state.accept_acks += 1;
            if let Some(chosen_entry) = state.get_chosen_entry(accept_message.first_unchosen_index)
            {
                let success_message = SuccessMessage {
                    index: accept_message.first_unchosen_index,
                    command: chosen_entry.command,
                };

                self.network
                    .send(from, Message::Success(success_message.clone()))
                    .await
                    .unwrap();
                debug!(
                    "Node {} sent success message {:?} to {}",
                    self.network.node, success_message, from
                );
            }
            if state.accept_acks == state.majority_threshold {
                if let Some(finish_accept) = state.finish_accept.take() {
                    finish_accept.send(()).unwrap();
                    debug!("Node {} finished accept phase", self.network.node);
                }
            }
        }
    }

    async fn process_success(&self, success_message: SuccessMessage, from: NodeId) {
        debug!(
            "Node {} got success message {:?} from {}",
            self.network.node, success_message, from
        );
        let mut state = self.state.lock().await;
        state.set_log(
            success_message.index,
            INFINITE_PROPOSAL.clone(),
            success_message.command,
        );

        let success_response = SuccessResponseMessage {
            first_unchosen_index: state.first_unchosen_index(),
        };

        self.network
            .send(from, Message::SuccessResponse(success_response.clone()))
            .await
            .unwrap();
        debug!(
            "Node {} sent success response {:?} to {}",
            self.network.node, success_response, from
        );
    }

    async fn process_success_response(
        &self,
        success_response_message: SuccessResponseMessage,
        from: NodeId,
    ) {
        debug!(
            "Node {} got success response {:?} from {}",
            self.network.node, success_response_message, from
        );
        let mut state = self.state.lock().await;
        if success_response_message.first_unchosen_index < state.first_unchosen_index() {
            let success_message = SuccessMessage {
                index: success_response_message.first_unchosen_index,
                command: state
                    .log
                    .get(success_response_message.first_unchosen_index)
                    .cloned()
                    .unwrap()
                    .unwrap()
                    .command
                    .clone(),
            };

            self.network
                .send(from, Message::Success(success_message.clone()))
                .await
                .unwrap();

            debug!(
                "Node {} sent success {:?} to {}",
                self.network.node, success_message, from
            );
        }
    }

    async fn process_read(&self, read_message: ReadMessage, from: NodeId) {
        debug!(
            "Node {} received read {:?} from {}",
            self.network.node, read_message, from
        );
        let mut state = self.state.lock().await;

        let read_response = ReadResponseMessage {
            id: read_message.id,
            chosen_sequence_end: read_message.chosen_sequence_end,
            missing_committed_entries: state
                .get_chosen_sequence_from(read_message.chosen_sequence_end + 1),
        };

        self.network
            .send(from, Message::ReadResponse(read_response.clone()))
            .await
            .unwrap();
        debug!(
            "Node {} sent read response {:?} to {}",
            self.network.node, read_response, from
        );
    }

    async fn process_read_response(&self, read_response: ReadResponseMessage, from: NodeId) {
        debug!(
            "Node {} recevied read response {:?} from {}",
            self.network.node, read_response, from
        );
        let mut state = self.state.lock().await;

        state.process_read_response(read_response);
    }
}

#[cfg(test)]
mod test {
    use crate::core::create_channel_network;
    use crate::multipaxos::{Command, CommandKind, Multipaxos, WriteCommand};
    use futures::future::JoinAll;
    use std::collections::HashMap;
    use std::time::Duration;

    #[tokio::test]
    async fn test_multipaxos() {
        let nodes = vec![0, 1, 2];
        let (mut virtual_networks, mut receivers) = create_channel_network(nodes.clone());

        let mut multipaxoses = HashMap::new();
        let mut spawned_tasks = Vec::new();
        for node in &nodes {
            let network = virtual_networks.remove(node).unwrap();
            let receive_channel = receivers.remove(node).unwrap();
            let (multipaxos, background_tasks) =
                Multipaxos::new(network, Duration::from_millis(10), receive_channel);
            multipaxoses.insert(node, multipaxos);
            spawned_tasks.push(background_tasks);
        }

        tokio::time::sleep(Duration::from_millis(21)).await;

        let result = multipaxoses[&2]
            .issue_command(Command {
                id: 1,
                client_id: 69,
                command_kind: CommandKind::Write(WriteCommand { key: 1, value: 10 }),
            })
            .await;

        let result = multipaxoses[&2]
            .issue_command(Command {
                id: 2,
                client_id: 69,
                command_kind: CommandKind::Write(WriteCommand { key: 2, value: 12 }),
            })
            .await;

        assert!(result.is_ok());

        let mut logs = vec![];

        for node in &nodes {
            logs.push(multipaxoses[node].read_log(0).await);
        }

        for node in &nodes {
            multipaxoses[node].quit();
        }
        spawned_tasks.into_iter().collect::<JoinAll<_>>().await;

        let first = &logs[0];
        assert!(logs.iter().all(|log| log == first))
    }
}
