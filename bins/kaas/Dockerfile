# syntax=docker/dockerfile:1.7
FROM rust:1.97-slim-bookworm AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --bin kaas && \
    cp target/release/kaas /kaas

FROM debian:bookworm-slim AS runtime
RUN useradd --system --uid 10001 kaas
COPY --from=build /kaas /usr/local/bin/kaas
USER 10001
EXPOSE 9092 9094 8080
ENTRYPOINT ["/usr/local/bin/kaas"]
