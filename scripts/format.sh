#! /bin/bash

cd ffi
taplo format --option "indent_string=    "
cargo fmt --all

cd ../cpp
find . -type f \( -name "*.h" -o -name "*.cc" \) -exec clang-format -i -style=file {} \;