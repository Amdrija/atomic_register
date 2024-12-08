use crate::command::{Command, CommandKind};
use crate::core;
use crate::core::{NodeId, Packet};
use crate::paxos::messages::{
    AcceptMessage, DecidedMessage, Message, PrepareMessage, PromiseMessage, ProposeMessage,
    RejectMessage,
};
use anyhow::anyhow;
use anyhow::Result;
use log::{debug, error};
use rkyv::{Archive, Deserialize, Serialize};
use tokio::sync::oneshot;

#[derive(Debug, Clone, Serialize, Deserialize, Archive, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProposalNumber {
    round: u64,
    node: NodeId,
}

pub const INFINITE_PROPOSAL: ProposalNumber = ProposalNumber {
    round: u64::MAX,
    node: NodeId::MAX,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Slot {
    pub promised: ProposalNumber,
    pub accepted: Option<(ProposalNumber, Command)>,
}

impl Slot {
    fn empty() -> Self {
        Self {
            promised: ProposalNumber { round: 0, node: 0 },
            accepted: None,
        }
    }
}

pub struct PaxosState {
    node: NodeId,
    majority_threshold: usize,
    nodes: Vec<NodeId>,
    log: Vec<Slot>,
    current_index: usize,
    current_proposal: ProposalNumber,
    prepare_acks: Vec<Option<(ProposalNumber, Command)>>,
    accept_acks: usize,
    decide: Option<oneshot::Sender<Command>>,
    default_value_to_propose: Command,
    proposed_value: Command,
}

impl PaxosState {
    pub fn new(node: NodeId, nodes: Vec<NodeId>) -> Self {
        Self {
            node,
            majority_threshold: nodes.len() / 2 + 1,
            nodes,
            log: vec![Slot {
                promised: INFINITE_PROPOSAL.clone(),
                accepted: Some((
                    INFINITE_PROPOSAL.clone(),
                    Command {
                        id: 0,
                        client_id: 0,
                        command_kind: CommandKind::NoOp,
                    },
                )),
            }],
            current_index: 0,
            current_proposal: ProposalNumber { round: 0, node },
            prepare_acks: vec![],
            accept_acks: 0,
            decide: None,
            default_value_to_propose: Command {
                id: 0,
                client_id: 0,
                command_kind: CommandKind::NoOp,
            },
            proposed_value: Command {
                id: 0,
                client_id: 0,
                command_kind: CommandKind::NoOp,
            },
        }
    }

    pub fn propose(
        &mut self,
        index: usize,
        command: Command,
    ) -> Result<(Vec<Packet<Message>>, oneshot::Receiver<Command>)> {
        self.default_value_to_propose = command.clone();
        let (send, recv) = oneshot::channel();
        self.decide.replace(send);

        let round = self
            .log
            .get(index)
            .map(|slot| slot.promised.round)
            .unwrap_or(0);
        // In theory, it is possible that 2 nodes livelock each other until everyone promises
        // INFINITY, and then the propose fails. In practice, even if the nodes managed to
        // complete a propose/prepare in 1ns, it would still take 31 years to fill u64, so
        // we are fine until ~2050.
        if round == u64::MAX {
            return Err(anyhow!(
                "Node {} already decied a value at index {}",
                self.node,
                index
            ));
        }

        Ok((self.go_to_propose(index, round + 1), recv))
    }

    fn go_to_propose(&mut self, index: usize, round: u64) -> Vec<Packet<Message>> {
        self.current_index = index;
        self.current_proposal = ProposalNumber {
            round,
            node: self.node,
        };
        self.prepare_acks.clear();
        self.accept_acks = 0;

        core::make_broadcast_packets(
            self.node,
            &self.nodes,
            Message::Prepare(PrepareMessage {
                index,
                proposal_number: self.current_proposal.clone(),
            }),
        )
    }

    pub fn process_prepare(
        &mut self,
        from: NodeId,
        prepare: PrepareMessage,
    ) -> Vec<Packet<Message>> {
        if prepare.index >= self.log.len() {
            self.set_log(prepare.index, Slot::empty());
        }

        // TODO: Maybe change this to be > instead of >=
        let message = if prepare.proposal_number >= self.log[prepare.index].promised {
            self.log[prepare.index].promised = prepare.proposal_number.clone();

            Message::Promise(PromiseMessage {
                index: prepare.index,
                proposal_number: prepare.proposal_number.clone(),
                accepted: self.log[prepare.index].accepted.clone(),
            })
        } else {
            let decided_value = if self.log[prepare.index].promised == INFINITE_PROPOSAL {
                Some(self.log[prepare.index].accepted.as_ref().unwrap().1.clone())
            } else {
                None
            };

            Message::Reject(RejectMessage {
                index: prepare.index,
                proposal_number: prepare.proposal_number,
                highest_proposal_number: self.log[prepare.index].promised.clone(),
                decided_value,
            })
        };

        vec![Packet {
            from: self.node,
            to: from,
            data: message,
        }]
    }

    pub fn process_promise(&mut self, promise: PromiseMessage) -> Vec<Packet<Message>> {
        if promise.proposal_number == self.current_proposal && promise.index == self.current_index {
            self.prepare_acks.push(promise.accepted);
            if self.prepare_acks.len() == self.majority_threshold {
                self.proposed_value = self
                    .prepare_acks
                    .iter()
                    .filter_map(|accepted_value| accepted_value.clone())
                    .max_by_key(|(proposal_number, _)| proposal_number.clone())
                    .map(|(_, value)| value)
                    .unwrap_or(self.default_value_to_propose.clone());
                self.accept_acks = 0;

                return core::make_broadcast_packets(
                    self.node,
                    &self.nodes,
                    Message::Propose(ProposeMessage {
                        index: self.current_index,
                        proposal_number: self.current_proposal.clone(),
                        value: self.proposed_value.clone(),
                    }),
                );
            }
        }

        Vec::new()
    }

    pub fn process_propose(
        &mut self,
        from: NodeId,
        propose: ProposeMessage,
    ) -> Vec<Packet<Message>> {
        if propose.index >= self.log.len() {
            self.set_log(propose.index, Slot::empty());
        }

        let message = if propose.proposal_number >= self.log[propose.index].promised {
            self.set_log(
                propose.index,
                Slot {
                    promised: propose.proposal_number.clone(),
                    accepted: Some((propose.proposal_number.clone(), propose.value.clone())),
                },
            );

            Message::Accept(AcceptMessage {
                index: propose.index,
                propsoal_number: propose.proposal_number,
            })
        } else {
            let decided_value = if self.log[propose.index].promised == INFINITE_PROPOSAL {
                Some(self.log[propose.index].accepted.as_ref().unwrap().1.clone())
            } else {
                None
            };

            Message::Reject(RejectMessage {
                index: propose.index,
                proposal_number: propose.proposal_number,
                highest_proposal_number: self.log[propose.index].promised.clone(),
                decided_value,
            })
        };

        vec![Packet {
            from: self.node,
            to: from,
            data: message,
        }]
    }

    pub fn process_accept(&mut self, accept: AcceptMessage) -> Vec<Packet<Message>> {
        if self.current_proposal == accept.propsoal_number && self.current_index == accept.index {
            self.accept_acks += 1;
            if self.accept_acks == self.majority_threshold {
                //Decide whichever value was proposed.
                return self.decide(self.current_index, self.proposed_value.clone());
            }
        }

        Vec::new()
    }

    pub fn process_reject(&mut self, reject_message: RejectMessage) -> Vec<Packet<Message>> {
        if reject_message.index == self.current_index
            && reject_message.proposal_number == self.current_proposal
        {
            if reject_message.highest_proposal_number == INFINITE_PROPOSAL {
                return self.decide(self.current_index, reject_message.decided_value.unwrap());
            }

            return self.go_to_propose(
                self.current_index,
                reject_message.highest_proposal_number.round + 1,
            );
        }

        Vec::new()
    }

    pub fn process_decide(&mut self, decide: DecidedMessage) -> Vec<Packet<Message>> {
        self.set_log(
            decide.index,
            Slot {
                promised: INFINITE_PROPOSAL.clone(),
                accepted: Some((INFINITE_PROPOSAL.clone(), decide.command)),
            },
        );

        Vec::new()
    }

    pub fn first_undecided_index(&self) -> usize {
        self.log
            .iter()
            .enumerate()
            .find(|(_, slot)| slot.promised < INFINITE_PROPOSAL)
            .map(|(index, _)| index)
            .unwrap_or(self.log.len())
    }

    pub fn get_log(&self) -> &Vec<Slot> {
        &self.log
    }

    fn set_log(&mut self, index: usize, slot: Slot) {
        if index >= self.log.len() {
            self.log.resize(index + 1, Slot::empty());
        }

        self.log[index] = slot;
    }

    fn decide(&mut self, index: usize, command: Command) -> Vec<Packet<Message>> {
        self.log[index] = Slot {
            promised: INFINITE_PROPOSAL.clone(),
            accepted: Some((INFINITE_PROPOSAL.clone(), command.clone())),
        };

        match self.decide.take() {
            None => {
                debug!("Node {} already decided at slot {}", self.node, index);
                Vec::new()
            }
            Some(decide) => {
                if let Err(_) = decide.send(command.clone()) {
                    error!("Node {} decided channel receiver has been dropped, probably due to finishing the propose.", self.node);
                    panic!("Node {} decided channel receiver has been dropped, probably due to finishing the propose.", self.node);
                }
                core::make_broadcast_packets(
                    self.node,
                    &self.nodes,
                    Message::Decide(DecidedMessage { index, command }),
                )
            }
        }
    }
}
