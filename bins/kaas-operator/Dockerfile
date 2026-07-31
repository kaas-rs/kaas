# syntax=docker/dockerfile:1.7
FROM rust:1.97-slim-bookworm AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --bin kaas-operator && \
    cp target/release/kaas-operator /kaas-operator

FROM debian:bookworm-slim AS runtime
RUN useradd --system --uid 10001 kaas
COPY --from=build /kaas-operator /usr/local/bin/kaas-operator
USER 10001
ENTRYPOINT ["/usr/local/bin/kaas-operator"]
