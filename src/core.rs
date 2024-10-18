use anyhow::{Ok, Result};
use log::info;
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
    T: Debug + Send + Sync + 'static,
{
    node: NodeId,
    senders: HashMap<NodeId, Sender<Packet<T>>>,
}

const SEND_CHANNEL_BUFFER: usize = 100;

impl<T> VirtualNetwork<T>
where
    T: Debug + Send + Sync + 'static,
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
        self.senders[&destination].send(packet).await?;

        Ok(())
    }
}

impl<T> Drop for VirtualNetwork<T>
where
    T: Debug + Send + Sync + 'static,
{
    fn drop(&mut self) {
        for (node_id, channel) in self.senders.drain() {
            info!("Closed Send part of channel for node {node_id}");
            drop(channel);
        }
    }
}

#[cfg(test)]
mod test {
    use crate::core::{NodeId, VirtualNetwork};

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
}
