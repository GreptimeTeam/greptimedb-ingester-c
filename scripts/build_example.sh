#!/usr/bin/env bash
set -euo pipefail

cwd="$(pwd)"
os="$(uname -s)"

case "${os}" in
Darwin)
    lib_name="libgreptime.dylib"
    ;;
Linux | FreeBSD)
    lib_name="libgreptime.so"
    ;;
*)
    echo "Unsupported platform: ${os}" >&2
    exit 1
    ;;
esac

mkdir -p "${cwd}/build"
cp "${cwd}/ffi/target/release/${lib_name}" "${cwd}/build/${lib_name}"

cc -g "${cwd}/c/src/main.c" -L"${cwd}/build" -lgreptime -o "${cwd}/build/ffi-example"
