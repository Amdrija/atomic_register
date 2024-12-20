use crate::command::Command;
use crate::multipaxos2::state::{LogEntry, ProposalNumber};
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct PrepareMessage {
    pub proposal_number: ProposalNumber,
    pub index: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct PromiseMessage {
    pub proposal_number: ProposalNumber,
    pub accepted: Option<LogEntry>,
    pub no_more_accepted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RejectPrepareMessage {
    pub proposal_number: ProposalNumber,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct ProposeMessage {
    pub proposal_number: ProposalNumber,
    pub index: usize,
    pub command: Command,
    pub first_unchosen_index: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct AcceptMessage {
    pub proposal_number: ProposalNumber,
    pub first_unchosen_index: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct SuccessMessage {
    pub index: usize,
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct SuccessResponseMessage {
    pub first_unchosen_index: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct ReadMessage {
    pub id: u64,
    pub chosen_sequence_end: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct ReadResponseMessage {
    pub id: u64,
    pub chosen_sequence_end: usize,
    pub missing_committed_entries: Vec<LogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub enum Message {
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
