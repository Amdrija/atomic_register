use crate::core::NodeId;
use anyhow::anyhow;
use anyhow::Result;
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;

#[derive(Deserialize, Debug, Clone)]
pub struct NodeAddress {
    pub source: SocketAddr,
    pub destination: SocketAddr,
}
#[derive(Deserialize, Debug)]
pub struct Config {
    pub my_node_id: NodeId,
    pub nodes: HashMap<NodeId, NodeAddress>,
}

impl Config {
    pub fn new() -> Result<Config> {
        let (my_node_id, config_path) = parse_args()?;
        let nodes = parse_network_config(&config_path)?;

        Ok(Config { my_node_id, nodes })
    }

    pub fn get_from_addresses(&self) -> HashMap<SocketAddr, NodeId> {
        self.nodes
            .iter()
            .map(|(node_id, node_address)| (node_address.source, *node_id))
            .collect()
    }
}

fn parse_args() -> Result<(NodeId, String)> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        return Err(anyhow!(
            "You must provide the NodeId and path to config file as command line arguments"
        ));
    }

    Ok((args[1].parse::<NodeId>()?, args[2].clone()))
}

fn parse_network_config(config_path: &str) -> Result<HashMap<NodeId, NodeAddress>> {
    let mut file = File::open(config_path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    let config: HashMap<String, NodeAddress> = toml::from_str(&contents)?;
    config
        .into_iter()
        .map(|(node_id, node_address)| -> Result<(NodeId, NodeAddress)> {
            Ok((node_id.parse::<NodeId>()?, node_address))
        })
        .collect()
}
