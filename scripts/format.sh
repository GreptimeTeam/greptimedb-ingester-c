#!/usr/bin/env bash

cwd="$(pwd)"

cd "${cwd}"/ffi
taplo format --option "indent_string=    "
cargo fmt --all

cd "${cwd}"/c/src
find . -iname '*.h' -o -iname '*.c' | xargs clang-format -i -style=file
