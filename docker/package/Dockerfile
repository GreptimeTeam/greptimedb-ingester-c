FROM greptime-registry.cn-hangzhou.cr.aliyuncs.com/greptime/dev-build-ubuntu18:latest AS builder

WORKDIR /build
COPY . .

RUN sh /build/scripts/build_so.sh

RUN sh /build/scripts/build_example.sh

FROM scratch
COPY --from=builder /build/build/libgreptimedb_client_cpp_ffi.so .
COPY --from=builder /build/build/ffi-example .
