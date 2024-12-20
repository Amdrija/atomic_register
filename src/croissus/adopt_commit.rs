use crate::command::Command;
use crate::core::{NodeId, Packet, VirtualNetwork};
use crate::croissus::flow::Flow;
use crate::croissus::messages::{
    AckMessage, DiffuseMessage, EchoMessage, LockMessage, LockReplyMessage, Message,
};
use crate::croissus::state::{CroissusResult, CroissusState};
use anyhow::{anyhow, Result};
use log::{debug, error, info};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

struct AdoptCommit {
    network: VirtualNetwork<Message>,
    state: Arc<Mutex<CroissusState>>,
    timeout: Duration,
    cancellation_token: CancellationToken,
}

impl AdoptCommit {
    pub fn new(
        network: VirtualNetwork<Message>,
        nodes: Vec<NodeId>,
        rtt: Duration,
        receive_channel: Receiver<Packet<Message>>,
    ) -> (Arc<Self>, JoinHandle<()>) {
        let node = network.node;

        let croissus = Arc::new(AdoptCommit {
            network,
            state: Arc::new(Mutex::new(CroissusState::new(
                node,
                nodes.clone(),
                Flow::ring(nodes.clone(), node),
            ))),
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

    pub async fn propose(&self, command: Command) -> (usize, CroissusResult) {
        let (index, result) = self.try_commit(command.clone()).await;
        (
            index,
            match result {
                Ok(_) => {
                    println!("COMMITTED {:?}", command);
                    CroissusResult::Committed(command)
                }
                Err(_) => {
                    let adopted_command = self.get_safe_value().await;
                    println!("ADOPTED {:?}", adopted_command);
                    CroissusResult::Adopted(self.get_safe_value().await)
                }
            },
        )
    }

    async fn try_commit(&self, command: Command) -> (usize, Result<()>) {
        let (packets, index, finished_propose) = {
            let mut state = self.state.lock().await;
            // Don't have to check if the slot is locked, since the propose will
            // automatically pick the next unlocked slot
            state.propose(command.clone())
        };

        let timeout = tokio::time::sleep(self.timeout);

        self.network.send_packets(packets).await;

        select! {
            _ = timeout => {
                debug!("Node {} ack timeout elapsed", self.network.node);
            }
            _ = finished_propose => {}
        }

        {
            let state = self.state.lock().await;
            if state.is_propose_committed() {
                return (index, Ok(()));
            }
        }

        (index, Err(anyhow!("Node couldn't commit, it must adopt")))
    }
    async fn get_safe_value(&self) -> Option<Command> {
        let (packets, lock_phase_finished) = {
            let mut state = self.state.lock().await;
            state.go_to_lock_phase()
        };

        self.network.send_packets(packets).await;

        match lock_phase_finished.await {
            Ok(command) => command,
            Err(error) => {
                error!("Node {} failed to finish lock phase, sender part of channel - finish_lock_phase dropped. Error: {}", self.network.node, error);
                panic!("{error}");
            }
        }
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
                        Message::Diffuse(diffuse) => self.process_diffuse(packet.from, diffuse).await,
                        Message::Echo(echo) => self.process_echo(packet.from, echo).await,
                        Message::Ack(ack) => self.process_ack(packet.from, ack).await,
                        Message::Lock(lock) => self.process_lock(packet.from, lock).await,
                        Message::LockReply(lock_reply) => self.process_lock_reply(packet.from, lock_reply).await
                    };
                }
                _ = self.cancellation_token.cancelled() => {
                    info!("Terminated receive loop");
                    return;
                }
            }
        }
    }

    async fn process_diffuse(&self, from: NodeId, diffuse: DiffuseMessage) {
        debug!(
            "Node {} received diffuse {:?} from {}",
            self.network.node, diffuse, from
        );
        let mut state = self.state.lock().await;

        let packets = state.process_diffuse(diffuse);
        self.network.send_packets(packets).await;
    }

    async fn process_echo(&self, from: NodeId, echo: EchoMessage) {
        debug!(
            "Node {} received echo {:?} from {}",
            self.network.node, echo, from
        );
        let mut state = self.state.lock().await;
        let packets = state.process_echo(from, echo);
        self.network.send_packets(packets).await;
    }

    async fn process_ack(&self, from: NodeId, ack: AckMessage) {
        debug!(
            "Node {} received ack {:?} from {}",
            self.network.node, ack, from
        );
        let mut state = self.state.lock().await;
        let packets = state.process_ack(from, ack);
        self.network.send_packets(packets).await;
    }

    async fn process_lock(&self, from: NodeId, lock: LockMessage) {
        debug!(
            "Node {} received lock {:?} from {}",
            self.network.node, lock, from
        );
        let mut state = self.state.lock().await;
        let packets = state.process_lock(from, lock);
        self.network.send_packets(packets).await;
    }

    async fn process_lock_reply(&self, from: NodeId, lock_reply: LockReplyMessage) {
        debug!(
            "Node {} received lock reply {:?} from {}",
            self.network.node, lock_reply, from
        );
        let mut state = self.state.lock().await;
        state.process_lock_reply(from, lock_reply);
    }
}

#[cfg(test)]
mod tests {
    use crate::command::{Command, CommandKind, WriteCommand};
    use crate::core::{create_channel_network, NodeId};
    use crate::croissus::adopt_commit::AdoptCommit;
    use crate::croissus::CroissusResult;
    use crate::logger_test_initializer;
    use futures::future::TryJoinAll;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::task::JoinHandle;

    fn init_test(
        nodes: &Vec<NodeId>,
    ) -> (
        HashMap<NodeId, Arc<AdoptCommit>>,
        TryJoinAll<JoinHandle<()>>,
    ) {
        logger_test_initializer::init_logger();
        let (mut virtual_networks, mut receivers) = create_channel_network(nodes.clone());

        let mut croissuses = HashMap::new();
        let mut spawned_tasks = Vec::new();
        for node in nodes {
            let network = virtual_networks.remove(node).unwrap();
            let receive_channel = receivers.remove(node).unwrap();
            let (croissus, join_handle) = AdoptCommit::new(
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
        let (index, croissus_result) = croissuses[&0].propose(proposed_command.clone()).await;

        assert_eq!(index, 1);
        match croissus_result {
            CroissusResult::Committed(command) => assert_eq!(proposed_command, command),
            CroissusResult::Adopted(_) => assert!(false),
        }

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

        let mut adopt_commit_log = HashMap::new();
        for proposer in &nodes {
            let (index, croissus_result) = croissuses[proposer]
                .propose(Command {
                    id: *proposer as u64,
                    client_id: *proposer as u64,
                    command_kind: CommandKind::Write(WriteCommand {
                        key: *proposer as u64,
                        value: 10 + *proposer as u32,
                    }),
                })
                .await;
            adopt_commit_log
                .entry(index)
                .or_insert(Vec::new())
                .push(croissus_result);
        }

        assert!(adopt_commit_log.values().all(|results| is_valid(results)));

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

        let c4 = croissuses.remove(&2).unwrap();
        let c3 = croissuses.remove(&5).unwrap();
        let c3_clone = c3.clone();
        let c4_clone = c4.clone();
        let c4_propose_task = tokio::spawn(async move {
            c4_clone
                .propose(Command {
                    id: 1,
                    client_id: 69,
                    command_kind: CommandKind::Write(WriteCommand { key: 1, value: 10 }),
                })
                .await
        });
        let c3_propose_task = tokio::spawn(async move {
            c3_clone
                .propose(Command {
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
        let (c4_index, c4_croissus_result) = c4_result.unwrap();

        let c3_result = c3_propose_task.await;
        assert!(c3_result.is_ok());
        let (c3_index, c3_croissus_result) = c3_result.unwrap();
        if c4_index == c3_index {
            assert!(is_valid(&vec![c4_croissus_result, c3_croissus_result]));
        } else {
            assert!(c4_croissus_result.is_committed());
            assert!(c3_croissus_result.is_committed());
        }

        // Cleanup
        c3.quit();
        c4.quit();
        for (_, croissus) in croissuses.drain() {
            croissus.quit();
        }
        spawned_tasks.await.unwrap();
    }

    fn is_valid(results: &Vec<CroissusResult>) -> bool {
        let committed = results
            .iter()
            .filter_map(|result| match result {
                CroissusResult::Committed(command) => Some(command),
                CroissusResult::Adopted(_) => None,
            })
            .collect::<Vec<_>>();

        if committed.len() > 1 {
            return false;
        }

        if committed.len() == 0 {
            return true;
        }

        let committed = committed[0];
        results.iter().all(|result| match result {
            CroissusResult::Committed(command) => command == committed,
            CroissusResult::Adopted(command) => command
                .as_ref()
                .map(|command| command == committed)
                .is_some(),
        })
    }
}
