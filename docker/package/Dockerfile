FROM greptime-registry.cn-hangzhou.cr.aliyuncs.com/greptime/dev-build-ubuntu18:latest AS builder

WORKDIR /build
COPY . .

RUN cd ffi && cargo build --release

FROM scratch
COPY --from=builder /build/ffi/target/release/libgreptimedb_client_cpp_ffi.so .
