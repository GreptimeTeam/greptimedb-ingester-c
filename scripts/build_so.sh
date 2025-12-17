#!/bin/bash
set -euo pipefail

cwd="$(pwd)"

cd ${cwd}/ffi
cargo build --release

mkdir -p "${cwd}/build"
cp "${cwd}/cpp/src/greptime.h" "${cwd}/build"
