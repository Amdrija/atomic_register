#!/usr/bin/env python3

import tomllib
import subprocess
import sys
import os

# TODO: Add network delay configuration
# TODO: Add support for arguments to run different builds, log_level, printing stderr and stdout to files etc.

mode = "debug"
log_level = "info"
config_path = "./config/config.toml"
print(f"Running nodes in {mode} mode")

with open(config_path, "rb") as config_file:
    config = tomllib.load(config_file)
    print(f"Loaded config {config}")

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

