ARG BUILDER_IMAGE=greptime-registry.cn-hangzhou.cr.aliyuncs.com/greptime/dev-build-ubuntu18:rust-1.92-20251217
FROM ${BUILDER_IMAGE} AS builder

WORKDIR /build
COPY . .

RUN bash /build/scripts/build_so.sh
RUN bash /build/scripts/build_example.sh

FROM scratch
COPY --from=builder /build/build/libgreptimedb_client_cpp_ffi.so .
COPY --from=builder /build/build/ffi-example .
