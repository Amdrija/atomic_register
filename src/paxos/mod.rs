use crate::command::Command;
use crate::core::{NodeId, Packet, VirtualNetwork};
use crate::paxos::messages::{
    AcceptMessage, DecidedMessage, Message, PrepareMessage, PromiseMessage, ProposeMessage,
    RejectMessage,
};
use crate::paxos::state::PaxosState;
use anyhow::{anyhow, Result};
use log::{debug, error, info};
use std::sync::Arc;
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub mod messages;
pub mod state;

pub struct Paxos {
    network: VirtualNetwork<Message>,
    state: Arc<Mutex<PaxosState>>,
    cancellation_token: CancellationToken,
}

impl Paxos {
    pub fn new(
        virtual_network: VirtualNetwork<Message>,
        nodes: Vec<NodeId>,
        receive_channel: Receiver<Packet<Message>>,
    ) -> (Arc<Self>, JoinHandle<()>) {
        let node = virtual_network.node;

        let paxos = Arc::new(Self {
            network: virtual_network,
            state: Arc::new(Mutex::new(PaxosState::new(node, nodes))),
            cancellation_token: CancellationToken::new(),
        });

        let paxos_clone = paxos.clone();
        let handle = tokio::spawn(async move { paxos_clone.receive_loop(receive_channel).await });

        (paxos, handle)
    }

    pub async fn propose_next(&self, command: Command) {
        loop {
            let index = self.state.lock().await.first_undecided_index();
            if let Ok(_) = self.propose(index, command.clone()).await {
                return;
            }
        }
    }

    pub async fn propose(&self, index: usize, command: Command) -> Result<()> {
        let (packets, decided) = {
            let mut state = self.state.lock().await;
            state.propose(index, command.clone())?
        };
        self.network.send_packets(packets).await;

        match decided.await {
            Ok(decided_command) => {
                if decided_command == command {
                    Ok(())
                } else {
                    Err(anyhow!("Decided different command"))
                }
            }
            Err(error) => {
                error!(
                    "Node {} received error while trying to propose. Error: {}",
                    self.network.node, error
                );
                Err(anyhow!(
                    "Node {} received error while trying to propose. Error: {}",
                    self.network.node,
                    error
                ))
            }
        }
    }

    pub fn quit(&self) {
        info!("Node {} shutting down", self.network.node);
        self.cancellation_token.cancel()
    }

    async fn receive_loop(&self, mut recv_channel: Receiver<Packet<Message>>) {
        debug!("Node {} initialized recv loop", self.network.node);
        loop {
            select! {
                result = recv_channel.recv() => {
                    if result.is_none() {
                        info!("Receive loop receiving channel closed");
                        return;
                    }

                    let packet = result.unwrap();
                    match packet.data {
                        Message::Prepare(prepare) => self.process_prepare(packet.from, prepare).await,
                        Message::Promise(promise) => self.process_promise(packet.from, promise).await,
                        Message::Propose(propose) => self.process_propose(packet.from, propose).await,
                        Message::Accept(accept) => self.process_accept(packet.from, accept).await,
                        Message::Reject(reject) => self.process_reject(packet.from, reject).await,
                        Message::Decide(decide) => self.process_decide(packet.from, decide).await,
                    };
                }
                _ = self.cancellation_token.cancelled() => {
                    info!("Terminated receive loop");
                    return;
                }
            }
        }
    }

    async fn process_prepare(&self, from: NodeId, prepare: PrepareMessage) {
        debug!(
            "Node {} received prepare message {:?} from {}",
            self.network.node, prepare, from
        );
        let mut state = self.state.lock().await;
        let packets = state.process_prepare(from, prepare);
        self.network.send_packets(packets).await;
    }

    async fn process_promise(&self, from: NodeId, promise: PromiseMessage) {
        debug!(
            "Node {} received promise message {:?} from {}",
            self.network.node, promise, from
        );
        let mut state = self.state.lock().await;
        let packets = state.process_promise(promise);
        self.network.send_packets(packets).await;
    }

    async fn process_propose(&self, from: NodeId, propose: ProposeMessage) {
        debug!(
            "Node {} received propose message {:?} from {}",
            self.network.node, propose, from
        );
        let mut state = self.state.lock().await;
        let packets = state.process_propose(from, propose);
        self.network.send_packets(packets).await;
    }

    async fn process_accept(&self, from: NodeId, accept: AcceptMessage) {
        debug!(
            "Node {} received accept message {:?} from {}",
            self.network.node, accept, from
        );
        let mut state = self.state.lock().await;
        let packets = state.process_accept(accept);
        self.network.send_packets(packets).await;
    }

    async fn process_reject(&self, from: NodeId, reject: RejectMessage) {
        debug!(
            "Node {} received reject message {:?} from {}",
            self.network.node, reject, from
        );
        let mut state = self.state.lock().await;
        let packets = state.process_reject(reject);
        self.network.send_packets(packets).await;
    }

    async fn process_decide(&self, from: NodeId, decide: DecidedMessage) {
        debug!(
            "Node {} received decide message {:?} from {}",
            self.network.node, decide, from
        );
        let mut state = self.state.lock().await;
        state.process_decide(decide);
    }
}

#[cfg(test)]
mod tests {
    use crate::command::{Command, CommandKind, WriteCommand};
    use crate::core::{create_channel_network, NodeId};
    use crate::logger_test_initializer;
    use crate::paxos::Paxos;
    use futures::future::JoinAll;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::task::JoinHandle;

    fn init_test(nodes: Vec<NodeId>) -> (HashMap<NodeId, Arc<Paxos>>, JoinAll<JoinHandle<()>>) {
        logger_test_initializer::init_logger();

        let (mut virtual_networks, mut receivers) = create_channel_network(nodes.clone());

        let (paxoses, handles): (Vec<_>, Vec<_>) = nodes
            .iter()
            .map(|node| {
                let (paxos, handle) = Paxos::new(
                    virtual_networks.remove(node).unwrap(),
                    nodes.clone(),
                    receivers.remove(node).unwrap(),
                );
                ((*node, paxos), handle)
            })
            .unzip();

        (paxoses.into_iter().collect(), handles.into_iter().collect())
    }

    #[tokio::test]
    async fn test_paxos_syncrhonous() {
        let nodes = vec![0, 1, 2, 3, 4];

        let (paxoses, join_task) = init_test(nodes.clone());

        tokio::time::sleep(Duration::from_millis(100)).await;
        for (node, paxos) in paxoses.iter() {
            let result = paxos
                .propose(
                    *node as usize + 1,
                    Command {
                        id: 1,
                        client_id: *node as u64,
                        command_kind: CommandKind::Write(WriteCommand {
                            key: 10 + *node as u64,
                            value: 20 + *node as u32,
                        }),
                    },
                )
                .await;
            assert!(result.is_ok());
        }

        assert!(all_logs_valid(0, &paxoses).await);

        paxoses.iter().for_each(|(_, paxos)| paxos.quit());
        join_task.await;
    }

    #[tokio::test]
    async fn test_paxos_concurrent() {
        let nodes = vec![0, 1, 2, 3, 4];

        let (paxoses, join_task) = init_test(nodes.clone());
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut handles = Vec::new();
        for node in nodes.clone() {
            let paxos_clone = paxoses[&node].clone();
            let handle = tokio::spawn(async move {
                paxos_clone
                    .propose(
                        1,
                        Command {
                            id: 1,
                            client_id: node as u64,
                            command_kind: CommandKind::Write(WriteCommand {
                                key: 10 + node as u64,
                                value: 20 + node as u32,
                            }),
                        },
                    )
                    .await
            });
            handles.push(handle);
        }
        handles.into_iter().collect::<JoinAll<_>>().await;

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(all_logs_valid(0, &paxoses).await);

        paxoses.iter().for_each(|(_, paxos)| paxos.quit());
        join_task.await;
    }

    #[tokio::test]
    async fn test_paxos_next_synchronous() {
        let nodes = vec![0, 1, 2, 3, 4];

        let (paxoses, join_task) = init_test(nodes.clone());

        tokio::time::sleep(Duration::from_millis(100)).await;
        for (node, paxos) in paxoses.iter() {
            paxos
                .propose_next(Command {
                    id: 1,
                    client_id: *node as u64,
                    command_kind: CommandKind::Write(WriteCommand {
                        key: 10 + *node as u64,
                        value: 20 + *node as u32,
                    }),
                })
                .await;
        }

        assert!(all_logs_valid(0, &paxoses).await);

        paxoses.iter().for_each(|(_, paxos)| paxos.quit());
        join_task.await;
    }

    #[tokio::test]
    async fn test_paxos_next_concurrent() {
        let nodes = vec![0, 1, 2, 3, 4];

        let (paxoses, join_task) = init_test(nodes.clone());
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut handles = Vec::new();
        for node in nodes.clone() {
            let paxos_clone = paxoses[&node].clone();
            let handle = tokio::spawn(async move {
                paxos_clone
                    .propose_next(Command {
                        id: 1,
                        client_id: node as u64,
                        command_kind: CommandKind::Write(WriteCommand {
                            key: 10 + node as u64,
                            value: 20 + node as u32,
                        }),
                    })
                    .await
            });
            handles.push(handle);
        }
        handles.into_iter().collect::<JoinAll<_>>().await;

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(all_logs_valid(0, &paxoses).await);

        paxoses.iter().for_each(|(_, paxos)| paxos.quit());
        join_task.await;
    }

    #[tokio::test]
    async fn test_paxos_propose_at_decided_index() {
        let nodes = vec![0, 1, 2, 3, 4];

        let (paxoses, join_task) = init_test(nodes.clone());
        tokio::time::sleep(Duration::from_millis(100)).await;

        let result = paxoses[&0]
            .propose(
                0,
                Command {
                    id: 0,
                    client_id: 0,
                    command_kind: CommandKind::Write(WriteCommand { key: 1, value: 1 }),
                },
            )
            .await;
        assert!(result.is_err());

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(all_logs_valid(0, &paxoses).await);

        paxoses.iter().for_each(|(_, paxos)| paxos.quit());
        join_task.await;
    }

    async fn all_logs_valid(node: NodeId, paxoses: &HashMap<NodeId, Arc<Paxos>>) -> bool {
        let log_0 = paxoses[&node].state.lock().await.get_log().clone();

        let mut logs = Vec::new();
        for (_, paxos) in paxoses.into_iter() {
            let log = paxos.state.lock().await.get_log().clone();
            logs.push(log);
        }
        logs.iter().all(|log| *log == log_0)
    }
}
