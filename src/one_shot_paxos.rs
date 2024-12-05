use crate::core::{NodeId, Packet, VirtualNetwork};
use anyhow::Result;
use log::info;
use rkyv::{Archive, Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::select;
use tokio::sync::oneshot::Sender;
use tokio::sync::{oneshot, Mutex};

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
struct PrepareMessage {
    round: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
struct PromiseMessage {
    round: u64,
    accepted: Option<(u64, u32)>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
struct ProposeMessage {
    round: u64,
    value: u32,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
struct AcceptMessage {
    round: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
struct RejectMessage {
    round: u64,
    highest_round: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
enum Message {
    Prepare(PrepareMessage),
    Promise(PromiseMessage),
    Propose(ProposeMessage),
    Accept(AcceptMessage),
    Reject(RejectMessage),
}

struct Paxos {
    virtual_network: VirtualNetwork<Message>,
    round: AtomicU64,
    promised: Arc<Mutex<u64>>,
    accepted: Arc<Mutex<Option<(u64, u32)>>>,
    prepare_acks: Arc<Mutex<Vec<Option<(u64, u32)>>>>,
    accept_acks: AtomicU64,
    decided: Arc<Mutex<Option<Sender<u32>>>>,
    default_value_to_propose: AtomicU32,
    proposed_value: AtomicU32,
}

impl Paxos {
    pub fn new(virtual_network: VirtualNetwork<Message>) -> Self {
        let node = virtual_network.node;
        Paxos {
            virtual_network,
            round: AtomicU64::new(node as u64),
            promised: Arc::new(Mutex::new(0)),
            accepted: Arc::new(Mutex::new(None)),
            prepare_acks: Arc::new(Mutex::new(Vec::new())),
            accept_acks: AtomicU64::new(0),
            decided: Arc::new(Mutex::new(None)),
            default_value_to_propose: AtomicU32::new(0),
            proposed_value: AtomicU32::new(0),
        }
    }

    pub async fn propose(&self, value: u32) -> Result<u32> {
        self.default_value_to_propose.store(value, Ordering::SeqCst);
        let (send, recv) = oneshot::channel();
        {
            self.decided.lock().await.replace(send);
        }
        self.send_prepare_message().await?;

        let decided_value = recv.await?;

        Ok(decided_value)
    }

    async fn send_prepare_message(&self) -> Result<()> {
        let round = self
            .round
            .fetch_add(self.virtual_network.node as u64, Ordering::SeqCst);
        let message = PrepareMessage {
            round: round + self.virtual_network.node as u64,
        };

        self.virtual_network
            .broadcast(Message::Prepare(message))
            .await?;

        Ok(())
    }

    pub async fn receive_loop(
        &self,
        mut recv_channel: tokio::sync::mpsc::Receiver<Packet<Message>>,
        mut quit_signal: tokio::sync::mpsc::Receiver<()>,
    ) {
        loop {
            select! {
                result = recv_channel.recv() => {
                    if result.is_none() {
                        info!("Receive loop receiving channel closed");
                        return;
                    }

                    let packet = result.unwrap();
                    match packet.data {
                        Message::Prepare(prepare) => {self.process_prepare_message(prepare, packet.from).await}
                        Message::Promise(promise) => {self.process_promise_message(promise).await}
                        Message::Propose(propose) => {self.process_propose_message(propose, packet.from).await}
                        Message::Accept(accept) => {self.process_accept_message(accept).await}
                        Message::Reject(reject) => {self.process_reject_message(reject).await}
                    };
                }
                _ = quit_signal.recv() => {
                    info!("Terminated receive loop");
                    return;
                }
            }
        }
    }

    async fn process_prepare_message(&self, message: PrepareMessage, from: NodeId) {
        let mut promised = self.promised.lock().await;
        if message.round > *promised {
            *promised = message.round;
            let accepted = { (*self.accepted.lock().await).clone() };

            let promise_message = PromiseMessage {
                round: message.round,
                accepted,
            };

            self.virtual_network
                .send(from, Message::Promise(promise_message))
                .await
                .unwrap();
        } else {
            let reject_message = RejectMessage {
                round: message.round,
                highest_round: *promised,
            };

            self.virtual_network
                .send(from, Message::Reject(reject_message))
                .await
                .unwrap();
        }
    }

    async fn process_promise_message(&self, message: PromiseMessage) {
        let mut prepare_acks = self.prepare_acks.lock().await;
        if message.round == self.round.load(Ordering::SeqCst) {
            prepare_acks.push(message.accepted);
            if prepare_acks.len() == self.virtual_network.len() / 2 + 1 {
                let proposed_value = prepare_acks
                    .iter()
                    .filter_map(|accepted_value| *accepted_value)
                    .max_by_key(|(accepted_round, _)| *accepted_round)
                    .map(|(_, value)| value)
                    .unwrap_or(self.default_value_to_propose.load(Ordering::SeqCst));
                self.proposed_value.store(proposed_value, Ordering::SeqCst);

                let propose_message = ProposeMessage {
                    round: message.round,
                    value: proposed_value,
                };

                self.virtual_network
                    .broadcast(Message::Propose(propose_message))
                    .await
                    .unwrap()
            }
        }
    }

    async fn process_propose_message(&self, message: ProposeMessage, from: NodeId) {
        let mut promised = self.promised.lock().await;
        if message.round >= *promised {
            *promised = message.round;
            {
                self.accepted
                    .lock()
                    .await
                    .replace((message.round, message.value));
            }

            let accept_message = AcceptMessage {
                round: message.round,
            };

            self.virtual_network
                .send(from, Message::Accept(accept_message))
                .await
                .unwrap()
        } else {
            let reject_message = RejectMessage {
                round: message.round,
                highest_round: self.round.load(Ordering::SeqCst),
            };

            self.virtual_network
                .send(from, Message::Reject(reject_message))
                .await
                .unwrap()
        }
    }

    async fn process_accept_message(&self, message: AcceptMessage) {
        if message.round == self.round.load(Ordering::SeqCst) {
            let previous_acks = self.accept_acks.fetch_add(1, Ordering::SeqCst);
            if previous_acks == self.virtual_network.len() as u64 / 2 {
                self.decided
                    .lock()
                    .await
                    .take()
                    .unwrap()
                    .send(self.proposed_value.load(Ordering::SeqCst))
                    .unwrap();
            }
        }
    }

    async fn process_reject_message(&self, message: RejectMessage) {
        let mut prepare_acks = self.prepare_acks.lock().await;
        if message.round == self.round.load(Ordering::SeqCst) {
            prepare_acks.clear();
            self.send_prepare_message().await.unwrap()
        }
    }
}

#[cfg(test)]
mod test {
    use crate::core::{create_channel_network, NodeId, Packet, VirtualNetwork};
    use crate::one_shot_paxos::{Message, Paxos};
    use futures::future::JoinAll;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::{Receiver, Sender};
    use tokio::task::JoinHandle;

    fn initialize_paxoses(
        nodes: Vec<NodeId>,
        mut virtual_networks: HashMap<NodeId, VirtualNetwork<Message>>,
        mut recv_channels: HashMap<NodeId, Receiver<Packet<Message>>>,
    ) -> (
        HashMap<NodeId, Arc<Paxos>>,
        Vec<JoinHandle<()>>,
        Vec<Sender<()>>,
    ) {
        let mut paxoses = HashMap::new();
        let mut receive_handles = Vec::new();
        let mut quit_signals = Vec::new();
        for node in nodes.clone() {
            let paxos = Arc::new(Paxos::new(virtual_networks.remove(&node).unwrap()));
            let recv = recv_channels.remove(&node).unwrap();
            let paxos_clone = paxos.clone();
            let (quit_send, quit_recv) = mpsc::channel(1);
            let receive_handle = tokio::spawn(async move {
                paxos_clone.receive_loop(recv, quit_recv).await;
            });

            receive_handles.push(receive_handle);
            paxoses.insert(node, paxos.clone());
            quit_signals.push(quit_send);
        }

        (paxoses, receive_handles, quit_signals)
    }

    #[tokio::test]
    async fn test_paxos_sync() {
        let nodes = vec![1, 2, 3, 4, 5];
        let (virtual_networks, recv_channels) = create_channel_network(nodes.clone());

        let (paxoses, receive_handles, quit_signals) =
            initialize_paxoses(nodes.clone(), virtual_networks, recv_channels);

        for node in &nodes {
            let result = paxoses[node].propose(*node as u32).await;
            assert!(result.is_ok());
            let decided = result.unwrap();
            assert_eq!(decided, nodes[0] as u32);
            println!("Node {node} decided: {decided}");
        }

        for quit_signal in quit_signals {
            quit_signal.send(()).await.unwrap();
        }
        drop(paxoses);
        receive_handles.into_iter().collect::<JoinAll<_>>().await;
    }

    #[tokio::test]
    async fn test_paxos_concurrent() {
        let nodes = vec![1, 2, 3, 4, 5];
        let (virtual_networks, recv_channels) = create_channel_network(nodes.clone());

        let (paxoses, receive_handles, quit_signals) =
            initialize_paxoses(nodes.clone(), virtual_networks, recv_channels);

        let mut propose_handles: Vec<JoinHandle<()>> = Vec::new();
        let (decide_send, mut decide_recv) = mpsc::channel(10);
        for node in nodes.clone() {
            let paxos_clone = paxoses[&node].clone();
            let decide_send_clone = decide_send.clone();
            let propose_handle = tokio::spawn(async move {
                let result = paxos_clone.propose(node as u32).await;
                assert!(result.is_ok());
                let decided = result.unwrap();
                println!("Node {node} decided: {decided}");
                decide_send_clone.send(decided).await.unwrap();
            });
            propose_handles.push(propose_handle);
        }
        drop(decide_send);
        //all must agree on the same value
        let mut decided = None;
        while let Some(new_decided) = decide_recv.recv().await {
            assert_eq!(new_decided, decided.unwrap_or(new_decided));
            decided.replace(new_decided);
        }
        assert!(decided.is_some());
        assert!(nodes.contains(&(decided.unwrap() as NodeId)));
        propose_handles.into_iter().collect::<JoinAll<_>>().await;
        for quit_signal in quit_signals {
            quit_signal.send(()).await.unwrap();
        }
        drop(paxoses);
        receive_handles.into_iter().collect::<JoinAll<_>>().await;
    }
}
