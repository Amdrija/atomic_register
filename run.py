#!/usr/bin/env python3

import tomllib
import subprocess
import sys
import os
import traceback

# TODO: Add support for arguments to run different builds, log_level, printing stderr and stdout to files etc.

def configure_network_delays(config, delays):
    subprocess.run(["pfctl", "-E"], check=True)

    pipe_number = 1
    node_delay_pipes = {}
    for node, to_delays in delays.items():
        if node not in config:
            raise Exception(f"Node {node} not in config")

        node_delay_pipes[node] = {}
        for to, delay in to_delays.items():
            if to not in config:
                raise Exception(f"Node {to} not in config")

            rc = subprocess.run(["dnctl", "pipe", str(pipe_number), "config", "delay", str(delay)], check=True)

            node_delay_pipes[node][to] = pipe_number
            pipe_number += 1
    subprocess.run(["dnctl", "show"])
    rules = ""
    for node, delay_pipes in node_delay_pipes.items():
        node_src_address = config[node]["source"].replace(":", " port ")
        node_dst_address = config[node]["destination"].replace(":", " port ")
        for to, pipe in delay_pipes.items():
            to_src_address = config[to]["source"].replace(":", " port ")
            to_dst_address = config[to]["destination"].replace(":", " port ")
            rules += f"dummynet out proto tcp from {node_src_address} to {to_dst_address} pipe {pipe}\n"
            rules += f"dummynet out proto tcp from {node_dst_address} to {to_src_address} pipe {pipe}\n"
    print(rules)
    cmd = subprocess.Popen(["pfctl", "-a", "ts", "-f", "-"], stdin=subprocess.PIPE)
    cmd.communicate(bytes(rules, "utf-8"))
    cmd.wait()
    if cmd.returncode != 0:
        raise subprocess.CalledProcessError(cmd.returncode, cmd.args)

def reset_network_config():
    subprocess.run(["pfctl", "-a", "ts", "-F", "all"], check = True)
    subprocess.run(["dnctl", "-q", "flush"], check = True)

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

reset_network_config()
try:
    configure_network_delays(config, delays)
except Exception as e:
    print(traceback.format_exc())
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
    print(errors)
    print()
    print(output)
    print("************")
    print()

reset_network_config()