pub mod messages;
pub mod state;

use crate::command::Command;
use crate::core::{NodeId, Packet, VirtualNetwork};
use crate::multipaxos2::messages::{
    AcceptMessage, Message, PrepareMessage, PromiseMessage, ProposeMessage, ReadMessage,
    ReadResponseMessage, RejectPrepareMessage, SuccessMessage, SuccessResponseMessage,
};
use crate::multipaxos2::state::{LogEntry, MultipaxosState};
use anyhow::{bail, Context, Result};
use log::{debug, info};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

pub struct Multipaxos {
    network: VirtualNetwork<Message>,
    heartbeat_delay: Duration,
    state: Arc<Mutex<MultipaxosState>>,
    cancellation_token: CancellationToken,
}

impl Multipaxos {
    pub fn new(
        virtual_network: VirtualNetwork<Message>,
        nodes: Vec<NodeId>,
        heartbeat_delay: Duration,
        receive_channel: Receiver<Packet<Message>>,
    ) -> (Arc<Self>, impl Future<Output = Result<()>>) {
        let node = virtual_network.node;

        let multipaxos = Arc::new(Multipaxos {
            network: virtual_network,
            heartbeat_delay,
            state: Arc::new(Mutex::new(MultipaxosState::new(node, nodes))),
            cancellation_token: CancellationToken::new(),
        });

        let multipaxos_clone = multipaxos.clone();
        let heartbeat_procssing_handle = tokio::spawn(async move {
            multipaxos_clone.heartbeat_processing_loop().await;
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
            heartbeat_handle.await?;
            heartbeat_procssing_handle.await?;
            receive_handle.await?;

            Ok(())
        };

        (multipaxos, cleanup_task)
    }

    // No point in issuing multiple concurrent commands
    // because the prepare phase will have to run for all,
    // and then they will potentially live-lock each other
    pub async fn issue_command(&self, command: Command) -> Result<()> {
        let prepared = {
            let state = self.state.lock().await;

            state.is_prepared()
        }?;

        let (index, command_to_propose) = if !prepared {
            let (packets, index, prepare_finished) = {
                let mut state = self.state.lock().await;
                state.go_to_prepare_state()
            };

            self.network.send_packets(packets).await;

            // If this await fails, that means that the send side was dropped
            // This is only possible if the prepare message was rejected by a node
            let accepted_entry = prepare_finished
                .await
                .with_context(|| "Prepare rejected from a node, try again")?;

            debug!("Node {} prepare successful", self.network.node);

            (
                index,
                accepted_entry
                    .map(|entry| entry.command)
                    .unwrap_or(command.clone()),
            )
        } else {
            let mut state = self.state.lock().await;
            info!("Node {} skipping prepare state", self.network.node);
            (state.skip_prepare_state(), command.clone())
        };

        let (packets, accept_finished) = {
            let mut state = self.state.lock().await;
            state.go_to_accept_phase(index, command_to_propose.clone())
        };
        self.network.send_packets(packets).await;
        debug!("Node {} sent proposals", self.network.node);
        accept_finished
            .await
            .with_context(|| "Proposal rejected from a node, try again")?;

        let packets = {
            let mut state = self.state.lock().await;
            state.finish_propose(index, command_to_propose.clone())
        };
        self.network.send_packets(packets).await;

        if command_to_propose != command {
            bail!("The proposal slot {} is taken, try again", index);
        }

        Ok(())
    }

    // TODO: This function might get stuck in theory or return wrong information
    // if the leader node crashes without informing a majority of nodes
    // that the entry has been chosen and none of the leaders afterwards try to propose.
    // This is possible because the current node could reads this chosen entry, but it will
    // then never be able to read a majority that agrees on the chosen entry (some have the new
    // one, while some have the old one). Or, it may read only the majority with the old chosen
    // entry, causing it to return the wrong end of the sequence. Anyways, we decided that
    // we don't handle this edge case. In reality, a way to fix this would be to return
    // a majority accepts, but I have to think about it a bit more (or that a leader proposes
    // a NoOp when it first becomes a leader).
    pub async fn read_log(&self, start: usize) -> Vec<LogEntry> {
        let chosen_sequence_end = {
            let state = self.state.lock().await;
            state.chosen_sequence_end()
        };

        loop {
            let (packets, finish_read_recv) = {
                let mut state = self.state.lock().await;
                state.go_to_read_phase(chosen_sequence_end)
            };
            self.network.send_packets(packets).await;

            let majority_equal = finish_read_recv.await.unwrap();
            if majority_equal {
                break;
            }
        }

        {
            let state = self.state.lock().await;
            state
                .get_log()
                .get(start..state.chosen_sequence_end() + 1)
                .unwrap_or_default()
                .iter()
                .cloned()
                .map(|entry| entry.unwrap())
                .collect()
        }
    }

    #[allow(dead_code)]
    pub async fn get_log(&self) -> Vec<Option<LogEntry>> {
        self.state.lock().await.get_log().clone()
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
                        Message::Prepare(prepare) => self.process_prepare(prepare, packet.from).await,
                        Message::RejectPrepare(reject_prepare) => self.process_reject_prepare(reject_prepare, packet.from).await,
                        Message::Promise(promise) => self.process_promise(promise, packet.from).await,
                        Message::Propose(propose) => self.process_propose(propose, packet.from).await,
                        Message::Accept(accept) => self.process_accept(accept, packet.from).await,
                        Message::Success(success) => self.process_success(success, packet.from).await,
                        Message::SuccessResponse(success_response) => self.process_success_response(success_response, packet.from).await,
                        Message::Read(read) => self.process_read(read, packet.from).await,
                        Message::ReadResponse(read_response) => self.process_read_response(read_response, packet.from).await,
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

    async fn heartbeat_processing_loop(&self) {
        let mut interval = tokio::time::interval(self.heartbeat_delay * 2);
        loop {
            select! {
                _ = interval.tick() => {
                    let mut state = self.state.lock().await;
                    state.elect_leader();
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
        let mut state = self.state.lock().await;
        state.process_heartbeat(from);
    }

    async fn process_prepare(&self, prepare_message: PrepareMessage, from: NodeId) {
        debug!(
            "Node {} processing prepare message {:?} from {}",
            self.network.node, prepare_message, from
        );
        let mut state = self.state.lock().await;
        let packets = state.process_prepare(from, prepare_message);
        debug!("Node {} sent {:?} to {}", self.network.node, packets, from);
        self.network.send_packets(packets).await;
    }

    async fn process_reject_prepare(
        &self,
        reject_prepare_message: RejectPrepareMessage,
        from: NodeId,
    ) {
        debug!(
            "Node {} received reject prepare {:?} from {}",
            self.network.node, reject_prepare_message, from
        );
        let mut state = self.state.lock().await;
        state.process_reject_prepare(from, reject_prepare_message);
    }

    async fn process_promise(&self, promise_message: PromiseMessage, from: NodeId) {
        debug!(
            "Node {} received promise message {:?} from {}",
            self.network.node, promise_message, from
        );
        let mut state = self.state.lock().await;
        state.process_promise(from, promise_message);
    }

    async fn process_propose(&self, propose_message: ProposeMessage, from: NodeId) {
        debug!(
            "Node {} received propose message {:?} from {}",
            self.network.node, propose_message, from
        );
        let mut state = self.state.lock().await;
        let packets = state.process_propose(from, propose_message);

        self.network.send_packets(packets).await;
    }

    async fn process_accept(&self, accept_message: AcceptMessage, from: NodeId) {
        debug!(
            "Node {} received accept message {:?} from {}",
            self.network.node, accept_message, from
        );
        let mut state = self.state.lock().await;
        let packets = state.process_accept(from, accept_message);
        self.network.send_packets(packets).await;
    }

    async fn process_success(&self, success_message: SuccessMessage, from: NodeId) {
        debug!(
            "Node {} got success message {:?} from {}",
            self.network.node, success_message, from
        );
        let mut state = self.state.lock().await;
        let packets = state.process_success(from, success_message);
        self.network.send_packets(packets).await;
    }

    async fn process_success_response(
        &self,
        success_response_message: SuccessResponseMessage,
        from: NodeId,
    ) {
        debug!(
            "Node {} got success response {:?} from {}",
            self.network.node, success_response_message, from
        );
        let state = self.state.lock().await;
        let packets = state.process_success_response(from, success_response_message);
        self.network.send_packets(packets).await;
    }

    async fn process_read(&self, read_message: ReadMessage, from: NodeId) {
        debug!(
            "Node {} received read {:?} from {}",
            self.network.node, read_message, from
        );
        let state = self.state.lock().await;

        let packets = state.process_read(from, read_message);
        self.network.send_packets(packets).await;
    }

    async fn process_read_response(&self, read_response: ReadResponseMessage, from: NodeId) {
        debug!(
            "Node {} recevied read response {:?} from {}",
            self.network.node, read_response, from
        );
        let mut state = self.state.lock().await;

        state.process_read_response(read_response);
    }
}

#[cfg(test)]
mod test {
    use crate::command::{Command, CommandKind, WriteCommand};
    use crate::core::create_channel_network;
    use crate::multipaxos2::Multipaxos;
    use futures::future::TryJoinAll;
    use std::collections::HashMap;
    use std::time::Duration;

    #[tokio::test]
    async fn test_multipaxos() {
        let nodes = vec![0, 1, 2];
        let (mut virtual_networks, mut receivers) = create_channel_network(nodes.clone());

        let mut multipaxoses = HashMap::new();
        let mut spawned_tasks = Vec::new();
        for node in &nodes {
            let network = virtual_networks.remove(node).unwrap();
            let receive_channel = receivers.remove(node).unwrap();
            let (multipaxos, background_tasks) = Multipaxos::new(
                network,
                nodes.clone(),
                Duration::from_millis(10),
                receive_channel,
            );
            multipaxoses.insert(node, multipaxos);
            spawned_tasks.push(background_tasks);
        }

        tokio::time::sleep(Duration::from_millis(21)).await;

        let result = multipaxoses[&2]
            .issue_command(Command {
                id: 1,
                client_id: 69,
                command_kind: CommandKind::Write(WriteCommand { key: 1, value: 10 }),
            })
            .await;
        assert!(result.is_ok());

        let result = multipaxoses[&2]
            .issue_command(Command {
                id: 2,
                client_id: 69,
                command_kind: CommandKind::Write(WriteCommand { key: 2, value: 12 }),
            })
            .await;

        assert!(result.is_ok());

        let mut logs = vec![];

        for node in &nodes {
            logs.push(multipaxoses[node].read_log(0).await);
        }

        for node in &nodes {
            multipaxoses[node].quit();
        }

        assert!(spawned_tasks
            .into_iter()
            .collect::<TryJoinAll<_>>()
            .await
            .is_ok());

        let first = &logs[0];
        assert!(logs.iter().all(|log| log == first))
    }
}
