#!/bin/bash
set -euo pipefail

cwd="$(pwd)"

mkdir -p "${cwd}/build"
cp "${cwd}/ffi/target/release/libgreptime.so" "${cwd}/build/libgreptime.so"

gcc -g "${cwd}/cpp/src/main.c" -L"${cwd}/build" -lgreptime -o "${cwd}/build/ffi-example"
