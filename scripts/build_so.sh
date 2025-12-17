#!/bin/bash

cwd="$(pwd)"

cd ${cwd}/ffi
cargo build --release

cp "${cwd}"/cpp/src/greptime.h "${cwd}"/build
