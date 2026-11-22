# Development image for the distributed order-processing system.
#
# It compiles every service executable. The compose file runs one container per
# executable; this keeps the development workflow simple while still giving each
# service its own process and lifecycle.
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        ninja-build \
        git \
        ca-certificates \
        pkg-config \
        libssl-dev \
        liblog4cxx-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app-src

COPY . .

# AMQP-CPP (used by rabbitmq-lib) is fetched by CMake at configure time.
RUN cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build -j"$(nproc)"

ENV RABBITMQ_HOST=rabbitmq

WORKDIR /app

CMD ["/app-src/build/order_service"]
