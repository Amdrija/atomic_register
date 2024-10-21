use crate::core::{NodeId, Packet, VirtualNetwork};
use anyhow::Result;
use log::info;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot::Sender;
use tokio::sync::{oneshot, Mutex};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Archive, Serialize, Deserialize)]
struct Timestamp {
    timestamp: u64,
    writer_id: NodeId,
    thread_id: usize,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct ReadMessage {
    operation_id: u64,
    key: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct ValueMessage {
    operation_id: u64,
    key: u64,
    timestamp: Timestamp,
    value: u32
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct WriteMessage {
    operation_id: u64,
    key: u64,
    timestamp: Timestamp,
    value: u32,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct WriteAck {
    operation_id: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub enum Message {
    ReadMessage(ReadMessage),
    ValueMessage(ValueMessage),
    WriteMessage(WriteMessage),
    WriteAck(WriteAck),
}

enum Operation {
    Read,
    Write,
}

pub struct ABD {
    next_operation_id: AtomicU64,
    network: VirtualNetwork<Message>,
    kv_store: Arc<Mutex<HashMap<u64, (Timestamp, u32)>>>,
    read_lists: Arc<Mutex<HashMap<u64, Vec<(Timestamp, u32)>>>>,
    acks: Arc<Mutex<HashMap<u64, u16>>>,
    operations: Arc<Mutex<HashMap<u64, Operation>>>,
    operation_end: Arc<Mutex<HashMap<u64, Sender<()>>>>,
    values_to_write: Arc<Mutex<HashMap<u64, u32>>>,
}

impl ABD {
    pub fn new(network: VirtualNetwork<Message>) -> Self {
        let abd = ABD {
            next_operation_id: AtomicU64::new(0),
            network,
            kv_store: Arc::new(Mutex::new(HashMap::new())),
            read_lists: Arc::new(Mutex::new(HashMap::new())),
            acks: Arc::new(Mutex::new(HashMap::new())),
            operations: Arc::new(Mutex::new(HashMap::new())),
            operation_end: Arc::new(Mutex::new(HashMap::new())),
            values_to_write: Arc::new(Mutex::new(HashMap::new())),
        };

        abd
    }

    async fn process_read_message(&self, message: ReadMessage, from: NodeId) {
        let (timestamp, value) = {
            let kv_guard = self.kv_store.lock().await;
            let ts_value = kv_guard.get(&message.key);
            match ts_value {
                None => (Timestamp {
                    timestamp: 0,
                    writer_id: self.network.node,
                    thread_id: thread_id::get()
                }, 0),
                Some(ts_value) => ts_value.clone()
            }
        };

        let value_message = ValueMessage {
            operation_id: message.operation_id,
            key: message.key,
            timestamp,
            value
        };

        self.network
            .send(from, Message::ValueMessage(value_message))
            .await
            .unwrap()
    }

    async fn process_value_message(&self, message: ValueMessage) {
        let operation_id = message.operation_id;
        let key = message.key;
        let quorum_value = self.get_quorum_value(message).await;

        if let Some((max_ts, mut value)) = quorum_value {
            let operations_guard = self.operations.lock().await;
            let timestamp = match operations_guard[&operation_id] {
                Operation::Read => {
                    // When reading, we want to remember the value biggest value we
                    // have read so far and write it to a majority
                    let mut values_to_write = self.values_to_write.lock().await;
                    *values_to_write.get_mut(&operation_id).unwrap() = value;

                    max_ts
                }
                Operation::Write => {
                    // When writing, we have to write the value to the majority
                    let mut value_to_write = self.values_to_write.lock().await;
                    value = value_to_write.remove(&operation_id).unwrap();

                    Timestamp {
                        timestamp: max_ts.timestamp + 1,
                        writer_id: self.network.node,
                        thread_id: thread_id::get(),
                    }
                }
            };

            let write_message = WriteMessage {
                operation_id,
                key,
                timestamp,
                value,
            };

            self.network
                .broadcast(Message::WriteMessage(write_message))
                .await
                .unwrap();
        }
    }

    async fn get_quorum_value(&self, message: ValueMessage) -> Option<(Timestamp, u32)> {
        let mut read_list_guard = self.read_lists.lock().await;
        let read_list = read_list_guard.get_mut(&message.operation_id);
        if let None = read_list {
            //Safely ignore because we have removed the read_list beforehand
            return None;
        }
        let read_list = read_list.unwrap();
        read_list.push((message.timestamp, message.value));

        if read_list.len() == self.network.len() / 2 + 1 {
            let max_ts_value = read_list
                .iter()
                .max_by(|(ts1, _), (ts2, _)| ts1.cmp(ts2))
                .unwrap()
                .clone();
            read_list_guard.remove(&message.operation_id);
            Some(max_ts_value)
        } else {
            None
        }
    }

    async fn process_write_message(&self, message: WriteMessage, from: NodeId) {
        let mut kv_store_guard = self.kv_store.lock().await;
        let current_value = kv_store_guard.get_mut(&message.key);
        match current_value {
            None => {
                kv_store_guard.insert(message.key, (message.timestamp, message.value));
            }
            Some(current_value) => {
                if message.timestamp > current_value.0 {
                    *current_value = (message.timestamp, message.value);
                }
            }
        }

        self.network
            .send(
                from,
                Message::WriteAck(WriteAck {
                    operation_id: message.operation_id,
                }),
            )
            .await
            .unwrap();
    }

    async fn process_ack_message(&self, message: WriteAck) {
        let mut ack_guard = self.acks.lock().await;
        let acks = ack_guard.get_mut(&message.operation_id);
        if let None = acks {
            // Safely ignore because the majority was already reached
            return;
        }
        let acks = acks.unwrap();
        *acks += 1;

        if *acks as usize > self.network.len() / 2 {
            ack_guard.remove(&message.operation_id);
            let mut operation_end = self.operation_end.lock().await;
            let operation_end = operation_end.remove(&message.operation_id).unwrap();
            operation_end.send(()).unwrap();
        }
    }

    pub async fn receive_loop(
        &self,
        mut recv_channel: Receiver<Packet<Message>>,
        mut quit_signal: Receiver<()>,
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
                        Message::ReadMessage(rm) => {self.process_read_message(rm, packet.from).await}
                        Message::ValueMessage(vm) => {self.process_value_message(vm).await}
                        Message::WriteMessage(wm) => {self.process_write_message(wm, packet.from).await}
                        Message::WriteAck(am) => {self.process_ack_message(am).await}
                    };
                }
                _ = quit_signal.recv() => {
                    info!("Terminated receive loop");
                    return;
                }
            }
        }
    }

    pub async fn read(&self, key: u64) -> Result<u32> {
        let (message, terminate) = self.initialize_operation(Operation::Read, key, 0).await;

        let operation_id = message.operation_id;
        self.network
            .broadcast(Message::ReadMessage(message))
            .await?;
        terminate.await?;
        let mut values_to_write = self.values_to_write.lock().await;
        let value = values_to_write.remove(&operation_id).unwrap();

        Ok(value)
    }

    pub async fn write(&self, key: u64, value: u32) -> Result<()> {
        let (message, terminate) = self.initialize_operation(Operation::Write, key, value).await;

        self.network
            .broadcast(Message::ReadMessage(message))
            .await?;
        terminate.await?;

        Ok(())
    }

    async fn initialize_operation(
        &self,
        operation: Operation,
        key: u64,
        value: u32,
    ) -> (ReadMessage, oneshot::Receiver<()>) {
        let message = ReadMessage {
            operation_id: self.next_operation_id.fetch_add(1, Ordering::SeqCst),
            key
        };

        {
            let mut read_lists = self.read_lists.lock().await;
            read_lists.insert(message.operation_id, Vec::new());
        }

        {
            let mut operation_guard = self.operations.lock().await;
            operation_guard.insert(message.operation_id, operation);
        }

        {
            let mut acks = self.acks.lock().await;
            acks.insert(message.operation_id, 0);
        }

        {
            let mut values_to_write = self.values_to_write.lock().await;
            values_to_write.insert(message.operation_id, value);
        }

        let (send, recv) = oneshot::channel();
        {
            let mut operation_end = self.operation_end.lock().await;
            operation_end.insert(message.operation_id, send);
        }

        (message, recv)
    }
}

#[cfg(test)]
mod tests {
    use crate::abd::{Message, ABD};
    use crate::core::{create_channel_network, NodeId, Packet, VirtualNetwork};
    use futures::future::JoinAll;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::{Receiver, Sender};
    use tokio::task::JoinHandle;

    fn initialize_abds(
        nodes: Vec<NodeId>,
        mut virtual_networks: HashMap<NodeId, VirtualNetwork<Message>>,
        mut recv_channels: HashMap<NodeId, Receiver<Packet<Message>>>,
    ) -> (
        HashMap<NodeId, Arc<ABD>>,
        Vec<JoinHandle<()>>,
        Vec<Sender<()>>,
    ) {
        let mut abds = HashMap::new();
        let mut receive_handles = Vec::new();
        let mut quit_signals = Vec::new();
        for node in nodes.clone() {
            let abd = Arc::new(ABD::new(virtual_networks.remove(&node).unwrap()));
            let recv = recv_channels.remove(&node).unwrap();
            let abd_clone = abd.clone();
            let (quit_send, quit_recv) = mpsc::channel(1);
            let receive_handle = tokio::spawn(async move {
                abd_clone.receive_loop(recv, quit_recv).await;
            });

            receive_handles.push(receive_handle);
            abds.insert(node, abd.clone());
            quit_signals.push(quit_send);
        }

        (abds, receive_handles, quit_signals)
    }

    #[tokio::test]
    async fn test_abd_sequential() {
        let nodes = vec![0, 1, 2, 3, 4];
        let (virtual_networks, recv_channels) = create_channel_network(nodes.clone());

        let (abds, receive_handles, quit_signals) =
            initialize_abds(nodes.clone(), virtual_networks, recv_channels);

        for node in &nodes {
            let write_value = (node * 10) as u32;
            abds[node].write(12, write_value).await.unwrap();
            for node in &nodes {
                let read = abds[node].read(12).await.unwrap();
                println!("Node {node} reads {read}");
                assert_eq!(read, write_value);
            }
        }

        for quit_signal in quit_signals {
            quit_signal.send(()).await.unwrap();
        }
        drop(abds);
        receive_handles.into_iter().collect::<JoinAll<_>>().await;
    }

    #[tokio::test]
    async fn test_abd_concurrent() {
        let nodes = vec![0, 1, 2, 3, 4];
        let (virtual_networks, recv_channels) = create_channel_network(nodes.clone());

        let (abds, receive_handles, quit_signals) =
            initialize_abds(nodes.clone(), virtual_networks, recv_channels);

        let mut write_handles = Vec::new();
        for node in &nodes {
            let abd = abds[node].clone();
            let node = *node;
            let write_handle = tokio::spawn(async move {
                let write_value = (node * 10) as u32;
                abd.write(14, write_value).await.unwrap();
                let read = abd.read(node as u64).await.unwrap();
                println!("Node {node} reads {read}");
            });
            write_handles.push(write_handle);
        }

        write_handles.into_iter().collect::<JoinAll<_>>().await;

        let mut read_values = HashSet::new();
        for node in &nodes {
            let read = abds[node].read(14).await.unwrap();
            println!("Node {node} reads {read}");
            read_values.insert(read);
        }

        assert_eq!(read_values.len(), 1);

        for quit_signal in quit_signals {
            quit_signal.send(()).await.unwrap();
        }
        drop(abds);
        receive_handles.into_iter().collect::<JoinAll<_>>().await;
    }

    #[tokio::test]
    async fn test_abd_sequential_different_keys() {
        let nodes = vec![0, 1, 2, 3, 4];
        let (virtual_networks, recv_channels) = create_channel_network(nodes.clone());

        let (abds, receive_handles, quit_signals) =
            initialize_abds(nodes.clone(), virtual_networks, recv_channels);

        for node in &nodes {
            let key = *node as u64;
            let write_value = (node * 10) as u32;
            abds[node].write(key, write_value).await.unwrap();
            for node in &nodes {
                let read = abds[node].read(key).await.unwrap();
                println!("Node {node} reads {read}");
                assert_eq!(read, write_value);
            }
        }

        for quit_signal in quit_signals {
            quit_signal.send(()).await.unwrap();
        }
        drop(abds);
        receive_handles.into_iter().collect::<JoinAll<_>>().await;
    }
}
