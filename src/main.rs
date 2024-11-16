use crate::abd::ABD;
use crate::config::Config;
use crate::multipaxos::{Command, CommandKind, Multipaxos, WriteCommand};
use crate::network::initialize_connections;
use anyhow::Result;
use log::{error, info};
use std::process::exit;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

mod abd;
mod config;
mod core;
mod multipaxos;
mod network;
mod paxos;

#[allow(dead_code)]
async fn run_paxos() -> Result<()> {
    info!("Started");
    let mut config = Config::new()?;
    info!("Finished parsing config: {:#?}", config);

    let (virtual_network, receiver_channel, read_tasks, send_loop_tasks) =
        initialize_connections(&config).await?;
    info!("Initialized connections to all nodes");

    let (multipaxos, looping_tasks) =
        Multipaxos::new(virtual_network, Duration::from_secs(1), receiver_channel);

    tokio::time::sleep(Duration::from_secs(5)).await;
    let leader = *config.nodes.keys().max().unwrap();
    if config.my_node_id == leader {
        let result = multipaxos
            .issue_command(Command {
                id: 22,
                client_id: config.my_node_id as u64,
                command_kind: CommandKind::Write(WriteCommand { key: 23, value: 29 }),
            })
            .await;

        if let Err(e) = result {
            info!("Error {e}");
        }
    } else {
        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    let log = multipaxos.read_log(0).await;
    println!("Log {:#?}", log);

    tokio::time::sleep(Duration::from_secs(5)).await;

    if config.my_node_id != leader {
        tokio::time::sleep(Duration::from_secs(10)).await;
        config.nodes.remove(&leader);
        let leader = *config.nodes.keys().max().unwrap();
        if config.my_node_id == leader {
            while let Err(error) = multipaxos
                .issue_command(Command {
                    id: 34,
                    client_id: config.my_node_id as u64,
                    command_kind: CommandKind::Write(WriteCommand { key: 35, value: 37 }),
                })
                .await
            {
                info!("Error {error}");
            }
        } else {
            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        let log = multipaxos.read_log(0).await;
        println!("Log {:#?}", log);

        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    info!("Initiated exit");
    multipaxos.quit();

    info!("Cleaning up and exiting");
    looping_tasks.await?;
    drop(multipaxos);

    read_tasks.await?.into_iter().collect::<Result<()>>()?;
    send_loop_tasks.await?.into_iter().collect::<Result<()>>()?;
    Ok(())
}

#[allow(dead_code)]
async fn run_abd() -> Result<()> {
    info!("Started");
    let config = Config::new()?;
    info!("Finished parsing config: {:#?}", config);

    let (virtual_network, receiver_channel, read_tasks, send_loop_tasks) =
        initialize_connections(&config).await?;
    info!("Initialized connections to all nodes");

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
    tokio::time::sleep(Duration::from_secs(5)).await;
    println!(
        "Node {}: experiment started at: {:.3}",
        config.my_node_id,
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs_f64()
    );

    let before_write = SystemTime::now();
    abd.write(1, 10 + config.my_node_id as u32).await?;
    let after_write = SystemTime::now();
    println!(
        "Node {} wrote key {}: {} time: {}ms",
        config.my_node_id,
        1,
        10 + config.my_node_id as u32,
        after_write.duration_since(before_write)?.as_millis()
    );

    for key in vec![1, 2] {
        let before_read = SystemTime::now();
        let read = abd.read(key).await?;
        let after_read = SystemTime::now();
        println!(
            "Node {} read key {}: {} time: {}ms",
            config.my_node_id,
            key,
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

    read_tasks.await?.into_iter().collect::<Result<()>>()?;
    send_loop_tasks.await?.into_iter().collect::<Result<()>>()?;

    Ok(())
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init_timed();
    if let Err(error) = run_paxos().await {
        error!("Error encountered while sending: {error}");
        exit(1);
    }
}
