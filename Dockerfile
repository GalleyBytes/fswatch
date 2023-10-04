FROM rust:1.68.0 AS builder
RUN apt update
RUN apt install -y musl-tools

# ------------------------------- -------------------------------
#          Build OpenSSL for the `musl` build target
# ------------------------------- -------------------------------
RUN \
  ln -s /usr/include/x86_64-linux-gnu/asm /usr/include/x86_64-linux-musl/asm && \
  ln -s /usr/include/asm-generic /usr/include/x86_64-linux-musl/asm-generic && \
  ln -s /usr/include/linux /usr/include/x86_64-linux-musl/linux

WORKDIR /musl

RUN wget https://github.com/openssl/openssl/archive/OpenSSL_1_1_1f.tar.gz
RUN tar zxvf OpenSSL_1_1_1f.tar.gz
WORKDIR /musl/openssl-OpenSSL_1_1_1f/

RUN CC="musl-gcc -fPIE -pie" ./Configure no-shared no-async --prefix=/musl --openssldir=/musl/ssl linux-x86_64
RUN make depend
RUN make -j$(nproc)
RUN make install

# ------------------------------- -------------------------------
#         Build the rust dependencies to speed cached builds
# ------------------------------- -------------------------------
WORKDIR /usr/src
# Download the target for static linking.
RUN rustup target add x86_64-unknown-linux-musl

# Create a dummy project and build the app's dependencies.
# If the Cargo.toml or Cargo.lock files have not changed,
# we can use the docker build cache and skip these (typically slow) steps.
RUN USER=root cargo new fswatch
WORKDIR /usr/src/fswatch
COPY Cargo.toml Cargo.lock ./
RUN cargo build --release

# ------------------------------- -------------------------------
#                Build the project from src
# ------------------------------- -------------------------------
ENV OPENSSL_DIR=/musl
COPY src ./src
RUN cargo install --target x86_64-unknown-linux-musl --path .

# ------------------------------- -------------------------------
#                              Bundle
# ------------------------------- -------------------------------
FROM scratch
COPY --from=builder /etc/ssl/certs /etc/ssl/certs
COPY --from=builder /usr/local/cargo/bin/fswatch .
CMD ["./fswatch"]
LABEL org.opencontainers.image.source https://github.com/galleybytes/fswatch
