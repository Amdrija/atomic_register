use crate::command::Command;
use crate::core::{make_broadcast_packets, NodeId, Packet, VirtualNetwork};
use crate::croissus::flow::Flow;
use crate::croissus::messages::Message as CroissusMessage;
use crate::croissus::state::{CroissusResult, CroissusState};
use crate::paxos::messages::{DecidedMessage, Message as PaxosMessage};
use crate::paxos::state::{PaxosState, INFINITE_PROPOSAL};
use anyhow::{anyhow, Result};
use log::{debug, error, info};
use rkyv::{Archive, Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

mod adopt_commit;
pub mod flow;
pub mod messages;
pub mod state;

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub enum HybridMessage {
    Paxos(PaxosMessage),
    Croissus(CroissusMessage),
}

struct Croissus {
    network: VirtualNetwork<HybridMessage>,
    nodes: Vec<NodeId>,
    croissus_state: Arc<Mutex<CroissusState>>,
    paxos_state: Arc<Mutex<PaxosState>>,
    decided_log: Arc<Mutex<Vec<Option<Command>>>>,
    timeout: Duration,
    cancellation_token: CancellationToken,
}

impl Croissus {
    pub fn new(
        network: VirtualNetwork<HybridMessage>,
        nodes: Vec<NodeId>,
        rtt: Duration,
        receive_channel: Receiver<Packet<HybridMessage>>,
    ) -> (Arc<Self>, JoinHandle<()>) {
        let node = network.node;

        let croissus = Arc::new(Croissus {
            network,
            nodes: nodes.clone(),
            croissus_state: Arc::new(Mutex::new(CroissusState::new(
                node,
                nodes.clone(),
                Flow::ring(nodes.clone(), node),
            ))),
            paxos_state: Arc::new(Mutex::new(PaxosState::new(node, nodes.clone()))),
            decided_log: Arc::new(Mutex::new(Vec::new())),
            timeout: rtt,
            cancellation_token: CancellationToken::new(),
        });

        let croissus_clone = croissus.clone();
        let recv_handle = tokio::spawn(async move {
            croissus_clone.receive_loop(receive_channel).await;
        });

        (croissus, recv_handle)
    }

    pub fn quit(&self) {
        info!("Node {} shutting down", self.network.node);
        self.cancellation_token.cancel()
    }

    pub async fn propose(&self, command: Command) {
        loop {
            let (index, result) = self.try_commit(command.clone()).await;
            if result.is_ok() {
                self.insert_decided(index, command.clone()).await;
                // Helps everyone else find the decided value
                self.network
                    .send_packets(make_broadcast_packets(
                        self.network.node,
                        &self.nodes,
                        HybridMessage::Paxos(PaxosMessage::Decide(DecidedMessage {
                            index,
                            command,
                        })),
                    ))
                    .await;
                return;
            }

            let adopted_command = self.get_safe_value().await.unwrap_or(command.clone());
            let paxos_result = self.paxos_propose(index, adopted_command).await;
            if let Ok(decided_command) = paxos_result {
                self.insert_decided(index, decided_command.clone()).await;
                if decided_command == command {
                    return;
                }
            }
        }
    }

    async fn try_commit(&self, command: Command) -> (usize, Result<()>) {
        let (packets, index, finished_propose) = {
            let mut state = self.croissus_state.lock().await;
            // Don't have to check if the slot is locked, since the propose will
            // automatically pick the next unlocked slot
            state.propose(command.clone())
        };

        let timeout = tokio::time::sleep(self.timeout);

        self.network
            .send_packets(Self::wrap_croissus_packets(packets))
            .await;

        select! {
            _ = timeout => {
                debug!("Node {} ack timeout elapsed", self.network.node);
            }
            _ = finished_propose => {}
        }

        {
            let state = self.croissus_state.lock().await;
            if state.is_propose_committed() {
                return (index, Ok(()));
            }
        }

        (index, Err(anyhow!("Node couldn't commit, it must adopt")))
    }
    async fn get_safe_value(&self) -> Option<Command> {
        let (packets, lock_phase_finished) = {
            let mut state = self.croissus_state.lock().await;
            state.go_to_lock_phase()
        };

        self.network
            .send_packets(Self::wrap_croissus_packets(packets))
            .await;

        match lock_phase_finished.await {
            Ok(command) => command,
            Err(error) => {
                error!("Node {} failed to finish lock phase, sender part of channel - finish_lock_phase dropped. Error: {}", self.network.node, error);
                panic!("{error}");
            }
        }
    }

    async fn paxos_propose(&self, index: usize, command: Command) -> Result<Command> {
        let (packets, decided) = {
            let mut state = self.paxos_state.lock().await;
            state.propose(index, command.clone())?
        };
        self.network
            .send_packets(Self::wrap_paxos_packets(packets))
            .await;

        let decided_command = decided.await?;
        Ok(decided_command)
    }

    async fn insert_decided(&self, index: usize, command: Command) {
        let mut decided_log = self.decided_log.lock().await;
        if index >= decided_log.len() {
            decided_log.resize(index + 1, None);
        }
        decided_log[index] = Some(command);
    }

    async fn receive_loop(&self, mut recv_channel: Receiver<Packet<HybridMessage>>) {
        debug!("Node {} initialized recv loop", self.network.node);
        loop {
            select! {
                result = recv_channel.recv() => {
                    if result.is_none() {
                        info!("Receive loop receiving channel closed");
                        return;
                    }

                    let packet = result.unwrap();
                    let packets_to_send = match packet.data {
                        HybridMessage::Croissus(croissus_message) => self.process_croissus_message(packet.from, croissus_message).await,
                        HybridMessage::Paxos(paxos_message) => self.process_paxos_message(packet.from, paxos_message).await,
                    };
                    self.network.send_packets(packets_to_send).await;

                }
                _ = self.cancellation_token.cancelled() => {
                    info!("Terminated receive loop");
                    return;
                }
            }
        }
    }

    async fn process_croissus_message(
        &self,
        from: NodeId,
        croissus_message: CroissusMessage,
    ) -> Vec<Packet<HybridMessage>> {
        debug!(
            "Node {} received message {:?} from {}",
            self.network.node, croissus_message, from
        );
        let mut croissus_state = self.croissus_state.lock().await;
        let packets = match croissus_message {
            CroissusMessage::Diffuse(diffuse) => croissus_state.process_diffuse(diffuse),
            CroissusMessage::Echo(echo) => croissus_state.process_echo(from, echo),
            CroissusMessage::Ack(ack) => croissus_state.process_ack(from, ack),
            CroissusMessage::Lock(lock) => croissus_state.process_lock(from, lock),
            CroissusMessage::LockReply(lock_reply) => {
                croissus_state.process_lock_reply(from, lock_reply)
            }
        };

        Self::wrap_croissus_packets(packets)
    }

    async fn process_paxos_message(
        &self,
        from: NodeId,
        paxos_message: PaxosMessage,
    ) -> Vec<Packet<HybridMessage>> {
        debug!(
            "Node {} received message {:?} from {}",
            self.network.node, paxos_message, from
        );
        let mut paxos_state = self.paxos_state.lock().await;
        let packets = match paxos_message {
            PaxosMessage::Prepare(prepare) => paxos_state.process_prepare(from, prepare),
            PaxosMessage::Promise(promise) => paxos_state.process_promise(promise),
            PaxosMessage::Propose(propose) => paxos_state.process_propose(from, propose),
            PaxosMessage::Accept(accept) => paxos_state.process_accept(accept),
            PaxosMessage::Reject(reject) => paxos_state.process_reject(reject),
            PaxosMessage::Decide(decide) => paxos_state.process_decide(decide),
        };

        Self::wrap_paxos_packets(packets)
    }

    fn wrap_croissus_packets(packets: Vec<Packet<CroissusMessage>>) -> Vec<Packet<HybridMessage>> {
        packets
            .into_iter()
            .map(|packet| Packet {
                from: packet.from,
                to: packet.to,
                data: HybridMessage::Croissus(packet.data),
            })
            .collect()
    }

    fn wrap_paxos_packets(packets: Vec<Packet<PaxosMessage>>) -> Vec<Packet<HybridMessage>> {
        packets
            .into_iter()
            .map(|packet| Packet {
                from: packet.from,
                to: packet.to,
                data: HybridMessage::Paxos(packet.data),
            })
            .collect()
    }

    pub async fn get_decided_log(&self) -> Vec<Option<Command>> {
        let mut decided_log = self.decided_log.lock().await.clone();
        let paxos_log = self.paxos_state.lock().await.get_log().clone();
        for (index, slot) in paxos_log.into_iter().enumerate() {
            if let Some((proposal, command)) = slot.accepted {
                if proposal == INFINITE_PROPOSAL {
                    if index >= decided_log.len() {
                        decided_log.resize(index + 1, None);
                    }
                    decided_log[index] = Some(command);
                }
            }
        }

        decided_log
    }
}

#[cfg(test)]
mod tests {
    use crate::command::{Command, CommandKind, WriteCommand};
    use crate::core::{create_channel_network, NodeId};
    use crate::croissus::Croissus;
    use crate::logger_test_initializer;
    use futures::future::TryJoinAll;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::task::JoinHandle;

    fn init_test(
        nodes: &Vec<NodeId>,
    ) -> (HashMap<NodeId, Arc<Croissus>>, TryJoinAll<JoinHandle<()>>) {
        logger_test_initializer::init_logger();
        let (mut virtual_networks, mut receivers) = create_channel_network(nodes.clone());

        let mut croissuses = HashMap::new();
        let mut spawned_tasks = Vec::new();
        for node in nodes {
            let network = virtual_networks.remove(node).unwrap();
            let receive_channel = receivers.remove(node).unwrap();
            let (croissus, join_handle) = Croissus::new(
                network,
                nodes.clone(),
                Duration::from_millis(10),
                receive_channel,
            );
            croissuses.insert(node.clone(), croissus);
            spawned_tasks.push(join_handle);
        }

        (
            croissuses,
            spawned_tasks.into_iter().collect::<TryJoinAll<_>>(),
        )
    }

    #[tokio::test]
    async fn test_running_alone() {
        let nodes = (0..9).into_iter().collect::<Vec<_>>();
        let (mut croissuses, spawned_tasks) = init_test(&nodes);

        let proposed_command = Command {
            id: 0,
            client_id: 0,
            command_kind: CommandKind::Write(WriteCommand { key: 10, value: 11 }),
        };
        croissuses[&0].propose(proposed_command.clone()).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(are_all_logs_same(&croissuses).await);

        // Cleanup
        for (_, croissus) in croissuses.drain() {
            croissus.quit();
        }
        spawned_tasks.await.unwrap();
    }

    #[tokio::test]
    async fn test_croissus_synchronous() {
        let nodes = (0..9).into_iter().collect::<Vec<_>>();
        let (mut croissuses, spawned_tasks) = init_test(&nodes);

        for proposer in &nodes {
            croissuses[proposer]
                .propose(Command {
                    id: *proposer as u64,
                    client_id: *proposer as u64,
                    command_kind: CommandKind::Write(WriteCommand {
                        key: *proposer as u64,
                        value: 10 + *proposer as u32,
                    }),
                })
                .await;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(are_all_logs_same(&croissuses).await);

        // Cleanup
        for (_, croissus) in croissuses.drain() {
            croissus.quit();
        }
        spawned_tasks.await.unwrap();
    }

    #[tokio::test]
    async fn test_croissus_concurrent() {
        let nodes = (0..9).into_iter().collect::<Vec<_>>();
        let (mut croissuses, spawned_tasks) = init_test(&nodes);

        let c3 = croissuses[&3].clone();
        let c4 = croissuses[&4].clone();
        let c4_propose_task = tokio::spawn(async move {
            c4.propose(Command {
                id: 1,
                client_id: 69,
                command_kind: CommandKind::Write(WriteCommand { key: 1, value: 10 }),
            })
            .await
        });
        let c3_propose_task = tokio::spawn(async move {
            c3.propose(Command {
                id: 2,
                client_id: 70,
                command_kind: CommandKind::Write(WriteCommand {
                    key: 12,
                    value: 102,
                }),
            })
            .await
        });

        let c4_result = c4_propose_task.await;
        assert!(c4_result.is_ok());

        let c3_result = c3_propose_task.await;
        assert!(c3_result.is_ok());

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(are_all_logs_same(&croissuses).await);

        // Cleanup
        for (_, croissus) in croissuses.drain() {
            croissus.quit();
        }
        spawned_tasks.await.unwrap();
    }

    async fn are_all_logs_same(croissuses: &HashMap<NodeId, Arc<Croissus>>) -> bool {
        let mut logs = Vec::new();
        for croissus in croissuses.values() {
            logs.push(croissus.get_decided_log().await);
        }

        logs.iter().all(|log| *log == logs[0])
    }
}
