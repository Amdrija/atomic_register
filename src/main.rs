use crate::config::Config;
use crate::network::initialize_connections;
use futures::future::{TryJoinAll};
use log::{error, info};
use rkyv::{Archive, Deserialize, Serialize};
use std::fmt::Debug;
use std::time::Duration;

mod config;
mod core;
mod network;

#[derive(Debug, Serialize, Deserialize, Archive)]
struct Message {
    text: String,
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

    let result = initialize_connections(&config).await;
    if let Err(error) = result {
        error!("Failed to initialize connections: {error}");
        return;
    }
    info!("Initialized connections to all nodes");
    let (virtual_network, mut receiver_channel, read_tasks, send_loop_tasks) = result.unwrap();

    let reader_handles = read_tasks
        .into_iter()
        .map(|read_task| tokio::spawn(read_task))
        .collect::<Vec<_>>();
    let sender_handles = send_loop_tasks
        .into_iter()
        .map(|send_task| tokio::spawn(send_task))
        .collect::<Vec<_>>();

    for i in 0..5 {
        virtual_network
            .send(
                1,
                Message {
                    text: String::from(format!("Hello {} from {}", i, config.my_node_id)),
                },
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let print_handle = tokio::spawn(async move {
        while let Some(message) = receiver_channel.recv().await {
            info!(
                "Node {} received message: {} from {}",
                message.to, message.data.text, message.from
            );
        }
    });

    //Cleanup
    info!("Cleaning up and exiting");
    drop(virtual_network);
    print_handle.await.unwrap();

    let read_result = reader_handles.into_iter().collect::<TryJoinAll<_>>().await;
    if let Err(error) = read_result {
        error!("Error encountered while reading: {error}");
    }

    let send_result = sender_handles.into_iter().collect::<TryJoinAll<_>>().await;
    if let Err(error) = send_result {
        error!("Error encountered while sending: {error}");
    }
}
