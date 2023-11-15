#! /bin/bash

cwd="$(pwd)"

[[ -d build ]] || mkdir build

cp "${cwd}"/ffi/target/debug/libgreptimedb_client_cpp_ffi.so build

gcc -g "${cwd}"/cpp/src/main.c -L"${cwd}"/build -lgreptimedb_client_cpp_ffi -o "${cwd}"/build/ffi-example