#!/bin/bash
set -euo pipefail

cwd="$(pwd)"

mkdir -p "${cwd}/build"
cp "${cwd}/ffi/target/release/libgreptimedb_client_cpp_ffi.so" "${cwd}/build"

gcc -g "${cwd}/cpp/src/main.c" -L"${cwd}/build" -lgreptimedb_client_cpp_ffi -o "${cwd}/build/ffi-example"
