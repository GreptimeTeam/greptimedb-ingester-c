#!/bin/bash
set -euo pipefail

cwd="$(pwd)"

cargo build --release --all-targets --manifest-path="${cwd}/ffi/Cargo.toml"

mkdir -p "${cwd}/build"
cp "${cwd}/cpp/src/greptime.h" "${cwd}/build"
