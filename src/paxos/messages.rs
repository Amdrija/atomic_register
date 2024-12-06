use crate::command::Command;
use crate::paxos::state::ProposalNumber;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct PrepareMessage {
    pub index: usize,
    pub proposal_number: ProposalNumber,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct PromiseMessage {
    pub index: usize,
    pub proposal_number: ProposalNumber,
    pub accepted: Option<(ProposalNumber, Command)>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct ProposeMessage {
    pub index: usize,
    pub proposal_number: ProposalNumber,
    pub value: Command,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct AcceptMessage {
    pub index: usize,
    pub propsoal_number: ProposalNumber,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct RejectMessage {
    pub index: usize,
    pub proposal_number: ProposalNumber,
    pub highest_proposal_number: ProposalNumber,
    pub decided_value: Option<Command>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct DecidedMessage {
    pub index: usize,
    pub command: Command,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub enum Message {
    Prepare(PrepareMessage),
    Promise(PromiseMessage),
    Propose(ProposeMessage),
    Accept(AcceptMessage),
    Reject(RejectMessage),
    Decide(DecidedMessage),
}
