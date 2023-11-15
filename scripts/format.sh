#! /bin/bash

cwd="$(pwd)"

cd "${cwd}"/ffi
taplo format --option "indent_string=    "
cargo fmt --all

cd "${cwd}"/cpp/src
find . -iname '*.h' -o -iname '*.c' | xargs clang-format -i -style=file