use crate::croissus::state::{LockedState, Proposal};
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct DiffuseMessage {
    pub index: usize,
    pub proposal: Proposal,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct EchoMessage {
    pub index: usize,
    pub proposal: Proposal,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct AckMessage {
    pub index: usize,
    pub proposal: Proposal,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct LockMessage {
    pub index: usize,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct LockReplyMessage {
    pub index: usize,
    pub locked_state: LockedState,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub enum Message {
    Diffuse(DiffuseMessage),
    Echo(EchoMessage),
    Ack(AckMessage), // Does an ack need to be identified?
    Lock(LockMessage),
    LockReply(LockReplyMessage),
}
