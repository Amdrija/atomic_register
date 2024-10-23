use std::process::exit;
use crate::abd::ABD;
use crate::config::Config;
use crate::network::initialize_connections;
use anyhow::Result;
use futures::future::TryJoinAll;
use log::{error, info};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

mod abd;
mod config;
mod core;
mod network;

async fn run() -> Result<()> {info!("Started");
    let config = Config::new()?;
    info!("Finished parsing config: {:#?}", config);

    let (virtual_network, receiver_channel, read_tasks, send_loop_tasks) = initialize_connections(&config).await?;
    info!("Initialized connections to all nodes");

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

    // Wait for everyone to connect because of different message propagation times
    // In theory, it shouldn't take more than max 1.5 * RTT time of all node connections
    info!(
        "Node {}: waiting 2s for everyone to connect",
        config.my_node_id
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    println!(
        "Node {}: experiment started at: {:.3}",
        config.my_node_id,
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs_f64()
    );

    if config.my_node_id == 2 {
        let before_write = SystemTime::now();
        abd.write(1, 10 + config.my_node_id as u32).await?;
        let after_write = SystemTime::now();
        println!(
            "Node {} wrote key {}: {} time: {}ms",
            config.my_node_id,
            1,
            10 + config.my_node_id as u32,
            after_write
                .duration_since(before_write)?
                .as_millis()
        );
    }

    for (node, _) in config.nodes {
        let before_read = SystemTime::now();
        let read = abd.read(node as u64).await?;
        let after_read = SystemTime::now();
        println!(
            "Node {} read key {}: {} time: {}ms",
            config.my_node_id,
            node,
            read,
            after_read.duration_since(before_read)?.as_millis()
        );
    }

    tokio::time::sleep(Duration::from_secs(5)).await;
    info!("Initiated exit");
    quit.send(()).await?;

    //Cleanup
    info!("Cleaning up and exiting");
    receive_loop_handle.await?;
    drop(abd);

    reader_handles.into_iter().collect::<TryJoinAll<_>>().await?.into_iter().collect::<Result<_>>()?;
    sender_handles.into_iter().collect::<TryJoinAll<_>>().await?.into_iter().collect::<Result<_>>()?;

    Ok(())
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init_timed();
    if let Err(error) = run().await {
        error!("Error encountered while sending: {error}");
        exit(1);
    }
}
