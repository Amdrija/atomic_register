use anyhow::{Ok, Result};
use log::{error, info};
use std::collections::HashMap;
use std::fmt::Debug;
use tokio::sync::mpsc::{self, Receiver, Sender};

pub type NodeId = u16;

#[derive(Debug)]
pub struct Packet<T: Debug> {
    pub from: NodeId,
    pub to: NodeId,
    pub data: T,
}

pub struct VirtualNetwork<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    pub node: NodeId,
    senders: HashMap<NodeId, Sender<Packet<T>>>,
}

const SEND_CHANNEL_BUFFER: usize = 100;

impl<T> VirtualNetwork<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    pub fn new(node: NodeId, nodes: Vec<NodeId>) -> (Self, HashMap<NodeId, Receiver<Packet<T>>>) {
        let mut senders = HashMap::new();
        let mut recvers = HashMap::new();
        for node_id in nodes {
            let (send_sender, send_recver) = mpsc::channel(SEND_CHANNEL_BUFFER);
            senders.insert(node_id, send_sender);
            recvers.insert(node_id, send_recver);
        }

        (VirtualNetwork { node, senders }, recvers)
    }

    pub async fn send(&self, destination: NodeId, data: T) -> Result<()> {
        let packet = Packet {
            from: self.node,
            to: destination,
            data,
        };
        let result = self.senders[&destination].send(packet).await;
        if let Err(error) = result {
            // Unfortunately, this error has to be swallowed, as calling unwrap will panic
            // the calling thread, which can possibly be the main thread or the thread responsible
            // for receiving messages, therefore, effectively killing the node (as it will not
            // be able to respond to any messages). This idea with channels wasn't so good :(
            // Best way to crash the nodes is to set up the link to drop packets instead of
            // actually crashing it.
            error!("Removing {} from network because the recv part of the channel is closed, likely due to closed connection, error: {}", destination, error);
        }

        Ok(())
    }

    pub async fn broadcast(&self, data: T) -> Result<()> {
        for node_id in self.senders.keys() {
            self.send(*node_id, data.clone()).await?;
        }

        Ok(())
    }

    pub async fn send_packets(&self, packets: Vec<Packet<T>>) {
        for packet in packets {
            let destination = packet.to;
            let result = self.senders[&packet.to].send(packet).await;
            if let Err(error) = result {
                // Unfortunately, this error has to be swallowed, as calling unwrap will panic
                // the calling thread, which can possibly be the main thread or the thread responsible
                // for receiving messages, therefore, effectively killing the node (as it will not
                // be able to respond to any messages). This idea with channels wasn't so good :(
                // Best way to crash the nodes is to set up the link to drop packets instead of
                // actually crashing them.
                error!("Removing {} from network because the recv part of the channel is closed, likely due to closed connection, error: {}", destination, error);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.senders.len()
    }
}

impl<T> Drop for VirtualNetwork<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    fn drop(&mut self) {
        for (node_id, channel) in self.senders.drain() {
            info!("Closed Send part of channel for node {node_id}");
            drop(channel);
        }
    }
}

#[cfg(test)]
pub fn create_channel_network<T>(
    nodes: Vec<NodeId>,
) -> (
    HashMap<NodeId, VirtualNetwork<T>>,
    HashMap<NodeId, Receiver<Packet<T>>>,
)
where
    T: Clone + Debug + Send + Sync + 'static,
{
    let mut send_channels = HashMap::new();
    let mut recv_channels = HashMap::new();
    let mut virtual_networks = HashMap::new();
    let mut network_recvs = HashMap::new();
    for node in nodes.clone() {
        let (node_send, node_recv) = mpsc::channel(20);
        send_channels.insert(node, node_send);
        recv_channels.insert(node, node_recv);

        let (vn, node_network_recvs) = VirtualNetwork::new(node, nodes.clone());
        virtual_networks.insert(node, vn);
        network_recvs.insert(node, node_network_recvs);
    }

    for node in nodes.clone() {
        let mut node_network_recvs = network_recvs.remove(&node).unwrap();
        for (to, mut network_recv) in node_network_recvs.drain() {
            let send = send_channels[&to].clone();
            tokio::spawn(async move {
                while let Some(packet) = network_recv.recv().await {
                    send.send(packet).await.unwrap()
                }
            });
        }
    }

    (virtual_networks, recv_channels)
}

pub fn make_broadcast_packets<T>(from: NodeId, nodes: &[NodeId], message: T) -> Vec<Packet<T>>
where
    T: Clone + Debug,
{
    nodes
        .iter()
        .map(|to| Packet {
            from,
            to: *to,
            data: message.clone(),
        })
        .collect()
}

#[cfg(test)]
mod test {
    use crate::core::{create_channel_network, NodeId, VirtualNetwork};
    use futures::future::JoinAll;
    use std::collections::HashSet;
    use std::vec;

    #[tokio::test]
    async fn test_network() {
        let nodes = vec![0, 1, 2, 3, 4];
        let node_id = 0;
        let (network, mut recvers) = VirtualNetwork::new(node_id, nodes.clone());
        let nodes_clone = nodes.clone();
        tokio::spawn(async move {
            for node in nodes {
                network
                    .send(node, node.to_string().into_bytes())
                    .await
                    .unwrap();
            }
        });

        let nodes = nodes_clone;
        for node in nodes {
            let recver = recvers.get_mut(&node);
            assert!(recver.is_some());
            let recver = recver.unwrap();
            let mut packet_count = 0;

            while let Some(packet) = recver.recv().await {
                packet_count += 1;
                assert_eq!(packet.from, node_id);
                assert_eq!(packet.to, node);
                assert_eq!(
                    node,
                    String::from_utf8(packet.data)
                        .unwrap()
                        .parse::<NodeId>()
                        .unwrap()
                );
            }

            assert_eq!(packet_count, 1);
        }
    }

    #[tokio::test]
    async fn test_channel_network() {
        let nodes = vec![0, 1, 2, 3, 4, 5];
        let (virtual_networks, node_recvs) = create_channel_network(nodes.clone());

        let mut send_handles = Vec::new();
        for (node, virtual_network) in virtual_networks {
            let send_handle = tokio::spawn(async move {
                virtual_network.broadcast(node).await.unwrap();
            });

            send_handles.push(send_handle);
        }

        let mut receive_handles = Vec::new();
        for (node, mut node_recv) in node_recvs {
            let mut nodes = nodes.iter().cloned().collect::<HashSet<_>>();
            let receive_handle = tokio::spawn(async move {
                while let Some(from_node) = node_recv.recv().await {
                    assert!(nodes.contains(&from_node.data));
                    assert!(nodes.contains(&from_node.from));
                    assert_eq!(from_node.to, node);
                    nodes.remove(&from_node.data);
                }

                assert!(nodes.is_empty());
            });

            receive_handles.push(receive_handle);
        }

        send_handles.into_iter().collect::<JoinAll<_>>().await;
        receive_handles.into_iter().collect::<JoinAll<_>>().await;
    }
}
