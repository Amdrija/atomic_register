use crate::config::Config;
use crate::network::initialize_connections;
use futures::future::TryJoinAll;
use log::{error, info};
use rkyv::{Archive, Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use crate::abd::ABD;

mod abd;
mod config;
mod core;
mod network;

#[derive(Clone, Debug, Serialize, Deserialize, Archive)]
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
    let (virtual_network, receiver_channel, read_tasks, send_loop_tasks) = result.unwrap();

    let reader_handles = read_tasks
        .into_iter()
        .map(|read_task| tokio::spawn(read_task))
        .collect::<Vec<_>>();
    let sender_handles = send_loop_tasks
        .into_iter()
        .map(|send_task| tokio::spawn(send_task))
        .collect::<Vec<_>>();

    let abd = Arc::new(ABD::new(virtual_network));
    let (quit, quit_signal) = mpsc::channel(1);
    let abd_clone = abd.clone();
    let receive_loop_handle = tokio::spawn(async move {
        abd_clone.receive_loop(receiver_channel, quit_signal).await;
    });

    tokio::time::sleep(Duration::from_secs(3 - config.my_node_id as u64)).await;
    abd.write(1, 10 + config.my_node_id as u32).await.unwrap();

    for node in config.nodes.keys() {
        info!("Node {} read key {}: {}", config.my_node_id, node, abd.read(*node as u64).await.unwrap());
    }

    tokio::time::sleep(Duration::from_secs(1)).await;
    info!("Initiated exit");
    quit.send(()).await.unwrap();

    //Cleanup
    info!("Cleaning up and exiting");
    receive_loop_handle.await.unwrap();
    drop(abd);

    let read_result = reader_handles.into_iter().collect::<TryJoinAll<_>>().await;
    if let Err(error) = read_result {
        error!("Error encountered while reading: {error}");
    }

    let send_result = sender_handles.into_iter().collect::<TryJoinAll<_>>().await;
    if let Err(error) = send_result {
        error!("Error encountered while sending: {error}");
    }
}
