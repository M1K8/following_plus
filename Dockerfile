# Dockerfile for Rust app
FROM rust:latest AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM gcr.io/distroless/cc-debian12
COPY --from=builder /app/target/release/m1kbsky /usr/local/bin/bsky
ENTRYPOINT ["bsky"]
