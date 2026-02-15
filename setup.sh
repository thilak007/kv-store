#!/bin/bash

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# uv manager
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env

# Run these inside the project directory
uv python install 3.12
uv sync

# apt install packages
sudo apt update
sudo apt install tree default-jre liblog4j2-java

cargo install just

sudo apt install golang-go