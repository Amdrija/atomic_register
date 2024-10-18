use std::env;
use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;
use log::{error, info};
use rkyv::{Archive, Deserialize, Serialize};
use tokio::sync::mpsc;
use crate::core::{NodeId, Packet, VirtualNetwork};
use anyhow::{anyhow, Error, Result};
use crate::config::Config;
use crate::network::{initialize_recv, initialize_send};

mod core;
mod network;
mod config;

#[derive(Debug, Serialize, Deserialize, Archive)]
struct Message {
    text: String
}



#[tokio::main]
async fn main() {
    pretty_env_logger::init_timed();

    let parse_result = Config::new();
    if let Err(error) = parse_result {
        error!("Error when parsing config: {error}");
        return;
    }
    let config = parse_result.unwrap();
    info!("Finished parsing config: {:#?}", config);


    let receiver_address = SocketAddr::from_str("127.0.0.1:56551").unwrap();
    let sender_address = SocketAddr::from_str("127.0.0.1:46441").unwrap();

    let (recv_send, mut recv_recv) = mpsc::channel::<Packet<Message>>(20);
    let senders = [(sender_address, recv_send)].iter().cloned().collect();
    let from_addresses = [(sender_address, 2)].iter().cloned().collect();
    let receiver_handle = tokio::spawn(initialize_recv(receiver_address, senders, 1, from_addresses));

    let (virtual_network, receivers) = VirtualNetwork::new(2, vec![1]);
    let mut senders = Vec::new();
    for (_, receiver) in receivers {
        let sender = initialize_send(sender_address, receiver_address, receiver).await;
        senders.push(sender);
    }

    let readers = receiver_handle.await.unwrap();
    match readers {
        Ok(readers) => {
            for reader in readers {
                tokio::spawn(reader);
            }
        }
        Err(error) => {
            error!("Failed to initialize receiver: {:#?}", error);
            return;
        }
    };

    let mut sender_handles = Vec::new();
    for sender in senders {
        let sender_handle = tokio::spawn(sender);
        sender_handles.push(sender_handle);
        for i in 0..10 {
            virtual_network.send(1, Message { text: String::from(format!("Hello {i}")) }).await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    let print_handle = tokio::spawn(async move {
        while let Some(message) = recv_recv.recv().await {
            info!("Received message: {}", message.data.text);
        }
    });

    drop(virtual_network);
    print_handle.await.unwrap();
    info!("Cleaning up and exiting");
    for sender_handle in sender_handles {
        match sender_handle.await.unwrap() {
            Ok(_) => {}
            Err(error) => {
                error!("Received error when sending: {:#?}", error);
            }
        };
    }
}
