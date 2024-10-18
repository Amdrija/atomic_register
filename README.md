# Atomic Register

This repo contains multiple implementations of various distributed algorithms for a replicated atomic register abstraction (or in layman's terms, a distributed key-value store).

The atomic register abstraction has 2 operations:
1. `READ(key)` which returns the value of the `key`.
2. `WRITE(key, value)` which sets the `key` to the `value`.

Unlike most commercial key-value stores, an important distinction of this project is that it is implementing [linearizable algorithms](https://jepsen.io/consistency/models/linearizable) (usually referred as Strong Consistency).

## Algorithms

- [ ] [ABD](https://dl.acm.org/doi/pdf/10.1145/200836.200869)
- [ ] [Paxos](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf)
- [ ] [Fast Paxos](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2005-112.pdf)
- [ ] [Raft](https://raft.github.io/)

## Boiler-plate implementation

The idea is very simple, abstract away the network layer (in this case TCP) completely behind channels. The algorithms will just take receive channels to receive messages and a "virtual" network of channels to send messages to appropriate nodes. This way, I can quickly implement different algorithms and test them on exactly the same network code. Another benefit of this is that the algorithms become easily testable, as now I can just emulate the network through the channels.

![Software Architecture](software_architecture.png)

The network is fully symmetric with each node connected to other nodes with 2 half-duplex TCP connections. Basically, each node has a port used to receive the messages and a port from which it sends messages. This is because it is not possible to bind 2 TCP sockets to the same port at the same time (so either a node can receive messages on the port, or connect to another node from a port). As a result, it is a lot more complex to build such a system using full-duplex connections, as the network isn't symmetrical, but some nodes must listen and some must connect. Consequently, there has to be some kind of ordering for initialization, which further complicates code.

![Network Architecture](network_ports.png)

## How to start the program

You have to specify two positional command line arguments in order for a node to start - `node_id` and `path_to_config_file`. An example command to start a node would be `atomic_register 1 config.toml`.

The config file is a `.toml` file which contains the mapping of `NodeId`s to the corresponding nodes source and destination ip addresses - you can take a look at the `config.toml` file in the repo for an example.