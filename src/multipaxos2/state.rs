use crate::command::{Command, CommandKind};
use crate::core;
use crate::core::{NodeId, Packet};
use crate::multipaxos2::messages::{
    AcceptMessage, Message, PrepareMessage, PromiseMessage, ProposeMessage, ReadMessage,
    ReadResponseMessage, RejectPrepareMessage, SuccessMessage, SuccessResponseMessage,
};
use anyhow::bail;
use anyhow::Result;
use log::{debug, info};
use rkyv::{Archive, Deserialize, Serialize};
use tokio::sync::oneshot;

#[derive(Debug, Clone, Serialize, Deserialize, Archive, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProposalNumber {
    pub round: u64,
    pub node: NodeId,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive, PartialEq, Eq)]
pub struct LogEntry {
    pub proposal_number: ProposalNumber,
    pub command: Command,
}

const INFINITE_PROPOSAL: ProposalNumber = ProposalNumber {
    round: u64::MAX,
    node: NodeId::MAX,
};

pub struct MultipaxosState {
    node: NodeId,
    nodes: Vec<NodeId>,
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
    pub fn new(node: NodeId, nodes: Vec<NodeId>) -> Self {
        let zero_proposal = ProposalNumber { round: 0, node };
        let majority_threshold = nodes.len() / 2 + 1;

        Self {
            node,
            nodes,
            majority_threshold,
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

    pub fn elect_leader(&mut self) {
        debug!("Node {} heard from: {:?}", self.node, self.heard_from);
        let highest_heard = self.heard_from.iter().max().unwrap_or(&self.node);
        self.leader = *highest_heard;
        debug!("Node {} elected {} for leader", self.node, self.leader);
        self.heard_from.clear();
    }

    pub fn process_heartbeat(&mut self, from: NodeId) -> Vec<Packet<Message>> {
        self.heard_from.push(from);
        vec![]
    }

    pub fn get_log(&self) -> &Vec<Option<LogEntry>> {
        &self.log
    }

    pub fn is_prepared(&self) -> Result<bool> {
        if self.leader != self.node {
            bail!("Current node not leader, leader={}", self.leader);
        }

        Ok(self.prepared)
    }

    pub fn go_to_prepare_state(
        &mut self,
    ) -> (
        Vec<Packet<Message>>,
        usize,
        oneshot::Receiver<Option<LogEntry>>,
    ) {
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

        let prepare_message = PrepareMessage {
            proposal_number,
            index,
        };

        (
            core::make_broadcast_packets(self.node, &self.nodes, Message::Prepare(prepare_message)),
            index,
            recv,
        )
    }

    pub fn skip_prepare_state(&mut self) -> usize {
        self.next_index += 1;
        self.next_index - 1
    }

    pub fn process_prepare(
        &mut self,
        from: NodeId,
        prepare_message: PrepareMessage,
    ) -> Vec<Packet<Message>> {
        if prepare_message.proposal_number >= self.min_promised_proposal {
            let accepted = self.promise(&prepare_message);
            let promise_message = PromiseMessage {
                proposal_number: prepare_message.proposal_number,
                accepted,
                no_more_accepted: self.no_more_accepted(prepare_message.index),
            };

            vec![Packet {
                from: self.node,
                to: from,
                data: Message::Promise(promise_message),
            }]
        } else {
            vec![Packet {
                from: self.node,
                to: from,
                data: Message::RejectPrepare(RejectPrepareMessage {
                    proposal_number: self.min_promised_proposal.clone(),
                }),
            }]
        }
    }

    pub fn process_reject_prepare(
        &mut self,
        from: NodeId,
        reject_prepare_message: RejectPrepareMessage,
    ) -> Vec<Packet<Message>> {
        if self.current_proposal < reject_prepare_message.proposal_number {
            self.fail_prepare_phase();
            debug!("Node {} failed prepare phase", self.node);
        }

        vec![]
    }

    pub fn process_promise(
        &mut self,
        from: NodeId,
        promise_message: PromiseMessage,
    ) -> Vec<Packet<Message>> {
        if promise_message.proposal_number != self.current_proposal {
            return vec![];
        }

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

        vec![]
    }

    pub fn go_to_accept_phase(
        &mut self,
        index: usize,
        command: Command,
    ) -> (Vec<Packet<Message>>, oneshot::Receiver<()>) {
        let (accept_send, accept_recv) = oneshot::channel();
        self.finish_accept.replace(accept_send);
        self.accept_acks = 0;

        let propose_message = ProposeMessage {
            proposal_number: self.current_proposal.clone(),
            index,
            command,
            first_unchosen_index: self.first_unchosen_index(),
        };
        (
            core::make_broadcast_packets(self.node, &self.nodes, Message::Propose(propose_message)),
            accept_recv,
        )
    }

    pub fn process_propose(
        &mut self,
        from: NodeId,
        propose_message: ProposeMessage,
    ) -> Vec<Packet<Message>> {
        if propose_message.proposal_number >= self.min_promised_proposal {
            self.min_promised_proposal = propose_message.proposal_number.clone();
            self.set_log(
                propose_message.index,
                propose_message.proposal_number.clone(),
                propose_message.command,
            );
            self.set_chosen_up_to(propose_message.index, &propose_message.proposal_number);
        }

        // An accept message acts both as an accept and as a reject message
        let accept_message = AcceptMessage {
            proposal_number: self.min_promised_proposal.clone(),
            first_unchosen_index: self.first_unchosen_index(),
        };
        vec![Packet {
            from: self.node,
            to: from,
            data: Message::Accept(accept_message),
        }]
    }

    pub fn process_accept(
        &mut self,
        from: NodeId,
        accept_message: AcceptMessage,
    ) -> Vec<Packet<Message>> {
        let mut packets = vec![];
        if accept_message.proposal_number > self.current_proposal {
            self.process_rejected_proposal(accept_message.proposal_number.round);
        } else if accept_message.proposal_number == self.current_proposal {
            self.accept_acks += 1;
            if let Some(chosen_entry) = self.get_chosen_entry(accept_message.first_unchosen_index) {
                let success_message = SuccessMessage {
                    index: accept_message.first_unchosen_index,
                    command: chosen_entry.command,
                };

                packets.push(Packet {
                    from: self.node,
                    to: from,
                    data: Message::Success(success_message),
                })
            }
            if self.accept_acks == self.majority_threshold {
                if let Some(finish_accept) = self.finish_accept.take() {
                    finish_accept.send(()).unwrap();
                    debug!("Node {} finished accept phase", self.node);
                }
            }
        }

        packets
    }

    pub fn process_success(
        &mut self,
        from: NodeId,
        success_message: SuccessMessage,
    ) -> Vec<Packet<Message>> {
        self.set_log_chosen(success_message.index, success_message.command);

        let success_response = SuccessResponseMessage {
            first_unchosen_index: self.first_unchosen_index(),
        };

        vec![Packet {
            from: self.node,
            to: from,
            data: Message::SuccessResponse(success_response),
        }]
    }

    pub fn process_success_response(
        &self,
        from: NodeId,
        success_response_message: SuccessResponseMessage,
    ) -> Vec<Packet<Message>> {
        if success_response_message.first_unchosen_index < self.first_unchosen_index() {
            let success_message = SuccessMessage {
                index: success_response_message.first_unchosen_index,
                command: self
                    .log
                    .get(success_response_message.first_unchosen_index)
                    .cloned()
                    .unwrap()
                    .unwrap()
                    .command
                    .clone(),
            };

            vec![Packet {
                from: self.node,
                to: from,
                data: Message::Success(success_message),
            }]
        } else {
            vec![]
        }
    }

    pub fn go_to_read_phase(
        &mut self,
        chosen_sequence_end: usize,
    ) -> (Vec<Packet<Message>>, oneshot::Receiver<bool>) {
        let read_message = ReadMessage {
            id: self.next_read_id,
            chosen_sequence_end,
        };
        self.next_read_id += 1;

        self.read_responses.clear();
        self.current_read_id = read_message.id;

        let (finish_read_send, finish_read_recv) = oneshot::channel();
        self.finish_read.replace(finish_read_send);

        (
            core::make_broadcast_packets(self.node, &self.nodes, Message::Read(read_message)),
            finish_read_recv,
        )
    }

    pub fn process_read(&self, from: NodeId, read_message: ReadMessage) -> Vec<Packet<Message>> {
        let read_response = ReadResponseMessage {
            id: read_message.id,
            chosen_sequence_end: read_message.chosen_sequence_end,
            missing_committed_entries: self
                .get_chosen_sequence_from(read_message.chosen_sequence_end + 1),
        };

        vec![Packet {
            from: self.node,
            to: from,
            data: Message::ReadResponse(read_response),
        }]
    }

    pub fn process_read_response(&mut self, read_response_message: ReadResponseMessage) {
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

    pub fn finish_propose(&mut self, index: usize, command: Command) -> Vec<Packet<Message>> {
        self.set_log_chosen(index, command.clone());

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
        let success_message = SuccessMessage { index, command };

        core::make_broadcast_packets(self.node, &self.nodes, Message::Success(success_message))
    }
    fn set_log_chosen(&mut self, index: usize, command: Command) {
        self.set_log(index, INFINITE_PROPOSAL.clone(), command);
    }

    fn process_rejected_proposal(&mut self, accepted_round: u64) {
        self.max_round = accepted_round;
        self.prepared = false;
        if let Some(finish_accept) = self.finish_accept.take() {
            self.accept_acks = 0;
            drop(finish_accept); // This will error the receiving side
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
            .find(|(_index, entry)| {
                entry.is_some() && entry.as_ref().unwrap().proposal_number < INFINITE_PROPOSAL
            })
            .map(|(index, _)| index)
            .unwrap_or(self.log.len())
    }

    pub fn chosen_sequence_end(&self) -> usize {
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
