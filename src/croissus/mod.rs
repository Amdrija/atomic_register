use crate::command::Command;
use crate::core::{NodeId, Packet, VirtualNetwork};
use crate::croissus::flow::Flow;
use crate::croissus::messages::{
    AckMessage, DiffuseMessage, EchoMessage, LockMessage, LockReplyMessage, MessageKind,
};
use crate::croissus::state::{CroissusResult, CroissusState, LockedState, Proposal, ProposalSlot};
use anyhow::{anyhow, bail, Result};
use log::{debug, error, info};
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::hash_map::Entry::Vacant;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot::Sender;
use tokio::sync::{oneshot, Mutex};
use tokio::sync::oneshot::error::RecvError;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub mod flow;
mod messages;
mod state;

struct Croissus {
    network: VirtualNetwork<MessageKind>,
    state: Arc<Mutex<CroissusState>>,
    timeout: Duration,
    cancellation_token: CancellationToken,
}

impl Croissus {
    pub fn new(
        network: VirtualNetwork<MessageKind>,
        nodes: Vec<NodeId>,
        rtt: Duration,
        receive_channel: Receiver<Packet<MessageKind>>,
    ) -> (Arc<Self>, JoinHandle<()>) {
        let node = network.node;

        let croissus = Arc::new(Croissus {
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

    async fn receive_loop(&self, mut recv_channel: Receiver<Packet<MessageKind>>) {
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
                        MessageKind::Diffuse(diffuse) => self.process_diffuse(packet.from, diffuse).await,
                        MessageKind::Echo(echo) => self.process_echo(packet.from, echo).await,
                        MessageKind::Ack(ack) => self.process_ack(packet.from, ack).await,
                        MessageKind::Lock(lock) => self.process_lock(packet.from, lock).await,
                        MessageKind::LockReply(lock_reply) => self.process_lock_reply(packet.from, lock_reply).await
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
        if ack.index >= state.log.len() {
            // TODO: Most likely not needed, since an ack has to come after we set proposal (as a response to diffuse)
            state.set_log(ack.index, ProposalSlot::None);
        }

        state.log[ack.index].as_mut().unwrap().acks.insert(from);

        if self.network.node == ack.proposal.proposer {
            // If the ack was delayed, ignore it
            if state.current_index != ack.index {
                return;
            }

            if state.can_ack(ack.index) {
                state.log[ack.index].as_mut().unwrap().done = true;
                match state.finish_adopt_commit_phase.take() {
                    None => {
                        error!(
                            "Node {} never set the send part for finish_propose",
                            self.network.node
                        );
                    }
                    Some(finish_propose) => {
                        if let Err(_) = finish_propose.send(()) {
                            debug!("Node {} recv part of finish_propose channel close - probably due to timeout", self.network.node);
                        }
                    }
                };
            }
        } else if let Some((destination, message)) = state.try_ack(ack.index, ack.proposal) {
            debug!("Node {} sending ack to {}", self.network.node, destination);
            self.network.send(destination, message).await.unwrap()
        }
    }

    async fn process_lock(&self, from: NodeId, lock: LockMessage) {
        debug!(
            "Node {} received lock {:?} from {}",
            self.network.node, lock, from
        );
        let mut state = self.state.lock().await;
        let locked_state = state.lock(lock.index);

        debug!(
            "Node {} sending lock reply {:?} to {}",
            self.network.node, locked_state, from
        );
        self.network
            .send(
                from,
                MessageKind::LockReply(LockReplyMessage {
                    index: lock.index,
                    locked_state,
                }),
            )
            .await
            .unwrap()
    }

    async fn process_lock_reply(&self, from: NodeId, lock_reply: LockReplyMessage) {
        debug!(
            "Node {} received lock reply {:?} from {}",
            self.network.node, lock_reply, from
        );
        let mut state = self.state.lock().await;
        if state.current_index != lock_reply.index {
            return;
        }
        state.process_lock_reply(from, lock_reply.locked_state);
    }

    pub async fn get_log(&self) -> Vec<Option<LockedState>> {
        self.state.lock().await.log.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::command::{Command, CommandKind, WriteCommand};
    use crate::core::create_channel_network;
    use crate::croissus::{Croissus, CroissusResult};
    use futures::future::TryJoinAll;
    use std::collections::HashMap;
    use std::env::set_var;
    use std::sync::Once;
    use std::time::Duration;

    static INIT_LOGGER: Once = Once::new();
    fn init_logger() {
        INIT_LOGGER.call_once(|| {
            unsafe {
                set_var("RUST_LOG", "TRACE");
            }
            pretty_env_logger::init_timed();
        });
    }

    #[tokio::test]
    async fn test_croissus_synchronous() {
        init_logger();
        let nodes = (0..9).into_iter().collect::<Vec<_>>();
        let (mut virtual_networks, mut receivers) = create_channel_network(nodes.clone());

        let mut croissuses = HashMap::new();
        let mut spawned_tasks = Vec::new();
        for node in &nodes {
            let network = virtual_networks.remove(node).unwrap();
            let receive_channel = receivers.remove(node).unwrap();
            let (croissus, join_handle) = Croissus::new(
                network,
                nodes.clone(),
                Duration::from_millis(100),
                receive_channel,
            );
            croissuses.insert(node.clone(), croissus);
            spawned_tasks.push(join_handle);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

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

            for other in nodes.iter().filter(|node| *node != proposer) {
                let (index, croissus_result) = croissuses[other]
                    .propose(Command {
                        id: *other as u64,
                        client_id: *other as u64,
                        command_kind: CommandKind::Write(WriteCommand {
                            key: *other as u64,
                            value: 10 + *other as u32,
                        }),
                    })
                    .await;
                adopt_commit_log
                    .entry(index)
                    .or_insert(Vec::new())
                    .push(croissus_result);
            }
        }

        assert!(adopt_commit_log.values().all(|results| is_valid(results)));

        for (_, croissus) in croissuses.drain() {
            croissus.quit();
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
        spawned_tasks
            .into_iter()
            .collect::<TryJoinAll<_>>()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_croissus_concurrent() {
        init_logger();
        let nodes = (0..9).into_iter().collect::<Vec<_>>();
        let (mut virtual_networks, mut receivers) = create_channel_network(nodes.clone());

        let mut croissuses = HashMap::new();
        let mut spawned_tasks = Vec::new();
        for node in &nodes {
            let network = virtual_networks.remove(node).unwrap();
            let receive_channel = receivers.remove(node).unwrap();
            let (croissus, join_handle) = Croissus::new(
                network,
                nodes.clone(),
                Duration::from_millis(100),
                receive_channel,
            );
            croissuses.insert(node.clone(), croissus);
            spawned_tasks.push(join_handle);
        }

        let c4 = croissuses.remove(&2).unwrap();
        let c3 = croissuses.remove(&5).unwrap();
        let c3_clone = c3.clone();
        let c4_clone = c4.clone();
        let f1 = tokio::spawn(async move {
            c4_clone
                .propose(Command {
                    id: 1,
                    client_id: 69,
                    command_kind: CommandKind::Write(WriteCommand { key: 1, value: 10 }),
                })
                .await
        });
        let f2 = tokio::spawn(async move {
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

        let result1 = f1.await;
        assert!(result1.is_ok());
        let (index1, result1) = result1.unwrap();

        let result2 = f2.await;
        assert!(result2.is_ok());
        let (index2, result2) = result2.unwrap();
        if index1 == index2 {
            assert!(is_valid(&vec![result1, result2]));
        } else {
            assert!(result1.is_committed());
            assert!(result2.is_committed());
        }

        c3.quit();
        c4.quit();
        for (_, croissus) in croissuses.drain() {
            croissus.quit();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        spawned_tasks
            .into_iter()
            .collect::<TryJoinAll<_>>()
            .await
            .unwrap();
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
