#! /bin/bash

cwd="$(pwd)"

cargo build --all-targets --manifest-path="${cwd}"/ffi/Cargo.toml
