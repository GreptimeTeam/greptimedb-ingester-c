#!/bin/bash

cwd="$(pwd)"

cargo build --all-targets --manifest-path="${cwd}"/ffi/Cargo.toml

cp "${cwd}"/cpp/src/greptime.h "${cwd}"/build
