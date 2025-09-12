FROM greptime-registry.cn-hangzhou.cr.aliyuncs.com/greptime/dev-build-ubuntu18:latest AS builder

WORKDIR /build
COPY . .

RUN cargo build --release --manifest-path ffi/Cargo.toml

FROM scratch
COPY --from=builder /build/ffi/target/release/libgreptimedb_client_cpp_ffi.so .
