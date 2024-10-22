use crate::config::Config;
use crate::core::{NodeId, Packet, VirtualNetwork};
use anyhow::{Context, Result};
use log::{debug, info};
use rkyv::api::high::{HighSerializer, HighValidator};
use rkyv::bytecheck::CheckBytes;
use rkyv::de::Pool;
use rkyv::rancor::{Error, Strategy};
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
use std::fmt::Debug;
use std::future::Future;
use std::time::Duration;
use std::{collections::HashMap, net::SocketAddr};
use tokio::sync::mpsc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket, TcpStream},
    sync::mpsc::{Receiver, Sender},
};

pub async fn initialize_connections<T>(
    config: &Config,
) -> Result<(
    VirtualNetwork<T>,
    Receiver<Packet<T>>,
    Vec<impl Future<Output = Result<()>>>,
    Vec<impl Future<Output = Result<()>>>,
)>
where
    T: Debug
        + Clone
        + Send
        + Sync
        + 'static
        + Archive
        + for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, Error>>,
    T::Archived:
        for<'a> CheckBytes<HighValidator<'a, Error>> + Deserialize<T, Strategy<Pool, Error>>,
{
    let (recv_send, recv_recv) = mpsc::channel::<Packet<T>>(20);
    let receiver_handle = tokio::spawn(initialize_recv(
        config.nodes[&config.my_node_id].destination,
        recv_send,
        config.my_node_id,
        config.get_from_addresses(),
    ));

    let (virtual_network, receivers) =
        VirtualNetwork::new(config.my_node_id, config.nodes.keys().cloned().collect());
    let mut send_loop_tasks = Vec::new();
    for (node_id, receiver) in receivers {
        let sender = initialize_send(
            config.nodes[&config.my_node_id].source,
            config.nodes[&node_id].destination,
            receiver,
        )
        .await;
        send_loop_tasks.push(sender);
    }

    let receiver_tasks = receiver_handle.await??;
    Ok((virtual_network, recv_recv, receiver_tasks, send_loop_tasks))
}

pub async fn initialize_send<T>(
    my_address: SocketAddr,
    destination_address: SocketAddr,
    recv: Receiver<Packet<T>>,
) -> impl Future<Output = Result<()>>
where
    T: Debug
        + Send
        + Sync
        + 'static
        + for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, Error>>,
{
    loop {
        let socket = TcpSocket::new_v4().unwrap();
        socket.set_reuseaddr(true).unwrap();
        socket.set_reuseport(true).unwrap();
        socket.bind(my_address).unwrap();
        if let Ok(stream) = socket.connect(destination_address).await {
            info!("From address {my_address} connected to {destination_address}");
            return send_loop(stream, recv, destination_address);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn send_loop<T>(
    mut stream: TcpStream,
    mut recv: Receiver<Packet<T>>,
    destination_address: SocketAddr,
) -> Result<()>
where
    T: Debug
        + Send
        + Sync
        + 'static
        + for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, Error>>,
{
    stream.set_nodelay(true)?;
    while let Some(packet) = recv.recv().await {
        let serialized = serialize(&packet.data)
            .with_context(|| format!("Failed to serialize packet: {:#?}", packet))?;
        debug!(
            "Node {} sending data to {}",
            stream.local_addr().unwrap(),
            stream.peer_addr().unwrap()
        );
        stream
            .write(&serialized)
            .await
            .with_context(|| format!("Failed to write data to address: {destination_address}"))?;
    }
    info!("Closed receive part of send channel for destination: {destination_address}");

    Ok(())
}

fn serialize<T>(payload: &T) -> Result<Vec<u8>>
where
    T: Debug
        + Send
        + Sync
        + 'static
        + for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, Error>>,
{
    let payload = rkyv::to_bytes::<Error>(payload)?;
    let len = payload.len();
    let mut bytes = vec![len as u8];
    bytes.extend_from_slice(payload.as_slice());

    Ok(bytes)
}

pub async fn initialize_recv<T>(
    receive_address: SocketAddr,
    sender: Sender<Packet<T>>,
    my_id: NodeId,
    mut from_addresses: HashMap<SocketAddr, NodeId>,
) -> Result<Vec<impl Future<Output = Result<()>>>>
where
    T: Debug + Send + Sync + 'static + Archive,
    T::Archived:
        for<'a> CheckBytes<HighValidator<'a, Error>> + Deserialize<T, Strategy<Pool, Error>>,
{
    let socket = TcpListener::bind(receive_address).await?;
    let mut futures = Vec::new();
    while !from_addresses.is_empty() {
        let (stream, source_address) = socket.accept().await?;

        // Ignore unknown connections
        if let Some(from_address) = from_addresses.remove(&source_address) {
            futures.push(read(stream, from_address, my_id, sender.clone()));
        }
    }
    info!(
        "Initialized connections from {receive_address} to nodes: {:#?}",
        from_addresses
    );

    Ok(futures)
}

async fn read<T>(
    mut stream: TcpStream,
    from: NodeId,
    to: NodeId,
    sender: Sender<Packet<T>>,
) -> Result<()>
where
    T: Debug + Send + Sync + 'static + Archive,
    T::Archived:
        for<'a> CheckBytes<HighValidator<'a, Error>> + Deserialize<T, Strategy<Pool, Error>>,
{
    info!(
        "Started reading from {} to {}",
        stream.peer_addr().unwrap(),
        stream.local_addr().unwrap()
    );
    let mut len_left = 0u8;
    let mut payload = Vec::new();
    loop {
        let mut buffer = [0u8; 100];
        let n = stream.read(&mut buffer).await?;
        if n == 0 {
            info!("Node {to} closed stream from node {from}");
            break;
        }
        debug!(
            "node {} received data from {}",
            stream.local_addr().unwrap(),
            stream.peer_addr().unwrap()
        );
        let (packets, new_len_left) =
            read_packets_from_buffer(&buffer, n, len_left as usize, &mut payload)?;
        len_left = new_len_left as u8;
        for packet in packets {
            sender
                .send(Packet {
                    from,
                    to,
                    data: packet,
                })
                .await?;
        }
    }

    Ok(())
}

fn read_packets_from_buffer<T>(
    buffer: &[u8],
    n: usize,
    mut len_left: usize,
    payload: &mut Vec<u8>,
) -> Result<(Vec<T>, usize)>
where
    T: Debug + Send + Sync + 'static + Archive,
    T::Archived:
        for<'a> CheckBytes<HighValidator<'a, Error>> + Deserialize<T, Strategy<Pool, Error>>,
{
    let mut packets = Vec::new();
    let mut i = 0;
    while i < n {
        if len_left == 0 {
            len_left = buffer[i] as usize;
            i += 1;
        }

        if n - i >= len_left {
            payload.extend_from_slice(&buffer[i..i + len_left]);
            packets.push(rkyv::from_bytes::<T, Error>(&payload)?);
            payload.clear();
            i += len_left;
            len_left = 0;
        } else {
            payload.extend_from_slice(&buffer[i..n]);
            len_left -= n - i;
            i = n;
        }
    }

    Ok((packets, len_left))
}

#[cfg(test)]
mod test {
    use crate::core::Packet;
    use crate::network::{initialize_recv, initialize_send, read_packets_from_buffer, serialize};
    use rkyv::rancor::Error;
    use rkyv::{Archive, Deserialize, Serialize};
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::str::FromStr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpSocket};
    use tokio::sync::mpsc::{Receiver, Sender};
    use tokio::sync::oneshot;
    use tokio::try_join;

    #[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
    struct TestPayload {
        id: u8,
        str: String,
    }

    #[tokio::test]
    pub async fn test_initialize_send() {
        let sender_address = SocketAddr::from_str("127.0.0.1:44441").unwrap();
        let receiver1_address = SocketAddr::from_str("127.0.0.1:55551").unwrap();
        let receiver2_address = SocketAddr::from_str("127.0.0.1:55552").unwrap();
        let payload1 = TestPayload {
            id: 1,
            str: String::from("Hello"),
        };
        let payload2 = TestPayload {
            id: 2,
            str: String::from("World"),
        };
        let payload1_clone = payload1.clone();
        let payload2_clone = payload2.clone();

        // Set up 2 TCP socket for receiving messages
        let (start1_send, start1_recv) = oneshot::channel();
        let (start2_send, start2_recv) = oneshot::channel();
        let mut receivers = Vec::new();
        for (receiver_address, payload, start) in vec![
            (receiver1_address, payload1, start1_send),
            (receiver2_address, payload2, start2_send),
        ] {
            let handle = tokio::spawn(async move {
                let listener = TcpListener::bind(receiver_address).await.unwrap();
                start.send(1).unwrap();
                let (mut stream, address) = listener.accept().await.unwrap();
                assert_eq!(address, sender_address);

                let mut buffer = [0u8; 100];
                let _n = stream.read(&mut buffer).await.unwrap();
                let len = buffer[0] as usize;
                let data = Vec::from(&buffer[1..len + 1]);
                assert_eq!(
                    payload,
                    rkyv::from_bytes::<TestPayload, Error>(&data).unwrap()
                );
            });
            receivers.push(handle);
        }

        start1_recv.await.unwrap();
        // Set up 2 senders
        let (sender1_send, sender1_recv) = tokio::sync::mpsc::channel(10);
        let sender1 = initialize_send(sender_address, receiver1_address, sender1_recv).await;
        let sender1 = tokio::spawn(sender1);

        start2_recv.await.unwrap();
        let (sender2_send, sender2_recv) = tokio::sync::mpsc::channel(10);
        let sender2 = initialize_send(sender_address, receiver2_address, sender2_recv).await;
        let sender2 = tokio::spawn(sender2);

        // Send data to senders
        let res = try_join!(
            sender1_send.send(Packet {
                from: 0,
                to: 1,
                data: payload1_clone
            }),
            sender2_send.send(Packet {
                from: 0,
                to: 2,
                data: payload2_clone
            })
        );
        assert!(res.is_ok());

        drop(sender1_send);
        drop(sender2_send);
        assert!(sender1.await.is_ok());
        assert!(sender2.await.is_ok());
        for handle in receivers {
            assert!(handle.await.is_ok());
        }
    }

    #[test]
    fn test_serialize() {
        let payload = TestPayload {
            id: 1,
            str: String::from("Hello"),
        };

        let bytes = serialize(&payload);
        assert!(bytes.is_ok());
        let bytes = bytes.unwrap();

        assert_eq!(bytes[0] as usize, bytes.len() - 1);
        let bytes = Vec::from(&bytes[1..]);
        let deserialized = rkyv::from_bytes::<TestPayload, Error>(&bytes);
        assert!(deserialized.is_ok());
        assert_eq!(deserialized.unwrap(), payload);
    }

    #[tokio::test]
    pub async fn test_initialize_recv() {
        let receiver_address = SocketAddr::from_str("127.0.0.1:55553").unwrap();
        let sender1_address = SocketAddr::from_str("127.0.0.1:44447").unwrap();
        let sender2_address = SocketAddr::from_str("127.0.0.1:44448").unwrap();
        let payload1 = TestPayload {
            id: 1,
            str: String::from("Hello"),
        };
        let payload2 = TestPayload {
            id: 2,
            str: String::from("World"),
        };
        let payload1_clone = payload1.clone();
        let payload2_clone = payload2.clone();

        let receiver_id = 0;
        let (recv_send, mut recv_recv): (
            Sender<Packet<TestPayload>>,
            Receiver<Packet<TestPayload>>,
        ) = tokio::sync::mpsc::channel(10);

        let mut from_addresses = HashMap::new();
        from_addresses.insert(sender1_address, 1);
        from_addresses.insert(sender2_address, 2);

        let readers = tokio::spawn(initialize_recv(
            receiver_address,
            recv_send,
            receiver_id,
            from_addresses,
        ));

        let mut senders = Vec::new();
        for (sender_address, payload) in
            vec![(sender1_address, payload1), (sender2_address, payload2)]
        {
            let handle = tokio::spawn(async move {
                loop {
                    let socket = TcpSocket::new_v4().unwrap();
                    socket.set_reuseport(true).unwrap();
                    socket.set_reuseaddr(true).unwrap();
                    socket.bind(sender_address).unwrap();
                    if let Ok(mut stream) = socket.connect(receiver_address).await {
                        let payload = serialize(&payload).unwrap();

                        stream.write(&payload).await.unwrap();
                        return;
                    }
                }
            });

            senders.push(handle);
        }

        let readers = readers.await.unwrap().unwrap();
        let mut reader_handles = Vec::new();
        for reader in readers {
            reader_handles.push(tokio::spawn(reader));
        }

        for sender in senders {
            assert!(try_join!(sender).is_ok());
        }

        let packet1 = recv_recv.recv().await;
        assert!(packet1.is_some());
        let packet1 = packet1.unwrap();
        let packet2 = recv_recv.recv().await;
        assert!(packet2.is_some());
        let packet2 = packet2.unwrap();
        let mut packets = vec![packet1, packet2];
        packets.sort_by(|p1, p2| p1.data.id.cmp(&p2.data.id));

        let packet1 = &packets[0];
        assert_eq!(packet1.from, 1);
        assert_eq!(packet1.to, receiver_id);
        assert_eq!(packet1.data, payload1_clone);

        let packet2 = &packets[1];
        assert_eq!(packet2.from, 2);
        assert_eq!(packet2.to, receiver_id);
        assert_eq!(packet2.data, payload2_clone);
        assert!(recv_recv.recv().await.is_none());

        for handle in reader_handles {
            assert!(try_join!(handle).is_ok());
        }
    }

    #[test]
    fn test_read_packets_from_buffer_multiple_packets_in_one_buffer() {
        let payload1 = TestPayload {
            id: 1,
            str: String::from("Hello"),
        };
        let payload2 = TestPayload {
            id: 2,
            str: String::from("World"),
        };

        let mut payload = serialize(&payload1).unwrap();
        payload.append(&mut serialize(&payload2).unwrap());

        let result = read_packets_from_buffer(&payload, payload.len(), 0, &mut Vec::new());
        assert!(result.is_ok());

        let (packets, len_left): (Vec<TestPayload>, usize) = result.unwrap();
        assert_eq!(len_left, 0);
        assert!(packets.iter().eq(vec![payload1, payload2].iter()));
    }

    #[test]
    fn test_read_packets_from_buffer_split_packet() {
        let payload = TestPayload {
            id: 1,
            str: String::from_utf8(vec![65; 150]).unwrap(),
        };

        let buffer = serialize(&payload).unwrap();

        let mut current_read = Vec::new();
        let result = read_packets_from_buffer(&buffer[..100], 100, 0, &mut current_read);
        assert!(result.is_ok());
        let (packets, len_left): (Vec<TestPayload>, usize) = result.unwrap();
        assert_eq!(packets.len(), 0);

        let result = read_packets_from_buffer(
            &buffer[100..],
            buffer.len() - 100,
            len_left,
            &mut current_read,
        );
        assert!(result.is_ok());
        let (packets, len_left): (Vec<TestPayload>, usize) = result.unwrap();
        assert_eq!(len_left, 0);
        assert!(packets.iter().eq(vec![payload].iter()));
    }
}
