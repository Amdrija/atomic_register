#!/usr/bin/env python3

import tomllib
import subprocess
import sys
import os

# TODO: Add network delay configuration
# TODO: Add support for arguments to run different builds, log_level, printing stderr and stdout to files etc.

def configure_network_delays(config, delays):
    rc = subprocess.run(["pfctl", "-E"])
    rc = 0
    if rc != 0:
        print("failed to run: pfctl -E")
        return rc

    pipe_number = 1
    node_delay_pipes = {}
    for node, to_delays in delays.items():
        if node not in config:
            print(f"Node {node} not in config")
            return 1

        node_delay_pipes[node] = {}
        for to, delay in to_delays.items():
            if to not in config:
                print(f"Node {to} not in config")
                return 1

#             rc = subprocess.run(["dnctl", "pipe", pipe_number, "config", "delay", delay])
            print(["dnctl", "pipe", pipe_number, "config", "delay", delay])
            rc = 0
            if rc != 0:
                return rc

            node_delay_pipes[node][to] = pipe_number
            pipe_number += 1

    rules = ""
    for node, delay_pipes in node_delay_pipes.items():
        src_address = config[node]["source"].replace(":", " port ")
        for to, pipe in delay_pipes.items():
            dst_address = config[to]["destination"].replace(":", " port ")
            rules += f"dummynet out proto tcp from {src_address} to port {dst_address} pipe 1\n"
            rules += f"dummynet out proto tcp from {dst_address} to port {src_address} pipe 1\n"
    print(rules)

def reset_network_config():
    subprocess.run(["pfctl", "-a", "ts", "-F", "all"])
    subprocess.run(["dnctl", "-q", "flush"])

mode = "debug"
log_level = "info"
config_path = "./config/config.toml"
delays_path = "./config/delays.toml"
print(f"Running nodes in {mode} mode")

with open(config_path, "rb") as config_file:
    config = tomllib.load(config_file)
    print(f"Loaded config {config}")

with open(delays_path, "rb") as delays_file:
    delays = tomllib.load(delays_file)
    print(f"Loaded delays {delays}")

rc = configure_network_delays(config, delays)
exit(0)
if rc != 0:
    reset_network_config()
    exit(1)

env = os.environ.copy()
env["RUST_LOG"] = log_level
processes = []
for node in config.keys():
    process = subprocess.Popen([f"./target/{mode}/atomic_register", node, config_path], env = env, stdout = subprocess.PIPE, stderr = subprocess.PIPE, text = True)
    processes.append(process)

print()
for process in processes:
    output, errors = process.communicate()
    rc = process.wait()
    print(output)
    if rc != 0 :
        print()
        print(errors)

reset_network_config()