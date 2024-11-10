use crate::core::{NodeId, Packet, VirtualNetwork};
use futures::future::JoinAll;
use log::{debug, info};
use rkyv::{Archive, Deserialize, Serialize};
use std::cmp::min;
use std::future::Future;
use std::sync::Arc;
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

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
struct WriteCommand {
    key: u64,
    value: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
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
    accepted: Option<LogEntry>,
    no_more_accepted: bool,
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

struct Multipaxos {
    network: VirtualNetwork<Message>,
    leader: Arc<Mutex<NodeId>>,
    leader_channel: Sender<NodeId>,
    heartbeat_delay: Duration,
    log: Arc<Mutex<Vec<LogEntry>>>,
    last_log_index: Arc<Mutex<usize>>,
    min_proposal: Arc<Mutex<ProposalNumber>>,
    max_round: Arc<Mutex<u64>>,
    next_index: Arc<Mutex<usize>>,
    prepared: Arc<Mutex<bool>>,
    cancellation_token: CancellationToken,
    heard_from: Arc<Mutex<Vec<NodeId>>>,
}

impl Multipaxos {
    fn new(
        virtual_network: VirtualNetwork<Message>,
        heartbeat_delay: Duration,
        receive_channel: Receiver<Packet<Message>>,
    ) -> (Arc<Self>, impl Future) {
        let min_proposal = ProposalNumber {
            round: 0,
            node: virtual_network.node,
        };

        let (leader_send, leader_receive) = mpsc::channel(20);
        let multipaxos = Arc::new(Multipaxos {
            network: virtual_network,
            leader: Arc::new(Mutex::new(min_proposal.node)),
            leader_channel: leader_send,
            heartbeat_delay,
            log: Arc::new(Mutex::new(vec![LogEntry {
                proposal_number: min_proposal.clone(),
                command: Command::NoOp,
            }])),
            last_log_index: Arc::new(Mutex::new(0)),
            min_proposal: Arc::new(Mutex::new(min_proposal)),
            max_round: Arc::new(Mutex::new(0)),
            next_index: Arc::new(Mutex::new(1)),
            prepared: Arc::new(Mutex::new(false)),
            cancellation_token: CancellationToken::new(),
            heard_from: Arc::new(Mutex::new(Vec::new())),
        });

        let multipaxos_clone = multipaxos.clone();
        let heartbeat_procssing_handle = tokio::spawn(async move {
            multipaxos_clone
                .heartbeat_processing_loop(leader_receive)
                .await;
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
                        Message::Prepare(_) => {}
                        Message::Promise(_) => {}
                        Message::Propose(_) => {}
                        Message::Accept(_) => {}
                        Message::Success(_) => {}
                        Message::SuccessResponse(_) => {}
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

    async fn heartbeat_processing_loop(&self, mut leader_receiver: Receiver<NodeId>) {
        let mut interval = tokio::time::interval(self.heartbeat_delay * 2);
        loop {
            select! {
                _ = interval.tick() => {
                    let mut leader = self.leader.lock().await;
                    let mut heard_from = self.heard_from.lock().await;
                    debug!("Node {} heard from: {:#?}", self.network.node, *heard_from);
                    let highest_heard = *heard_from.iter().max().unwrap_or(&self.network.node);
                    *leader = highest_heard;
                    debug!("Node {} elected {} for leader", self.network.node, *leader);
                    heard_from.clear();
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
        let mut heard_from = self.heard_from.lock().await;
        heard_from.push(from);
    }
}

#[cfg(test)]
mod test {
    use crate::core::create_channel_network;
    use crate::multipaxos::Multipaxos;
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
                Multipaxos::new(network, Duration::from_millis(10), receive_channel);
            multipaxoses.insert(node, multipaxos);
            spawned_tasks.push(background_tasks);
        }

        tokio::time::sleep(Duration::from_millis(32)).await;

        let mp4 = multipaxoses.remove(&4).unwrap();
        mp4.quit();
        drop(mp4);
        debug!("***** KILLED 4 *****");

        tokio::time::sleep(Duration::from_millis(66)).await;

        for node in &nodes {
            multipaxoses[node].quit();
        }

        spawned_tasks.into_iter().collect::<JoinAll<_>>().await;

        assert!(false);
    }
}
