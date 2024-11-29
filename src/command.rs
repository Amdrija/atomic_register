use rkyv::{Archive, Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Archive, PartialEq, Eq)]
pub struct WriteCommand {
    pub key: u64,
    pub value: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive, PartialEq, Eq)]
pub enum CommandKind {
    NoOp,
    Write(WriteCommand),
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive, PartialEq, Eq)]
pub struct Command {
    pub id: u64,
    pub client_id: u64,
    pub command_kind: CommandKind,
}
