
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    clang \
    clang-format \
    clang-tidy \
    cmake \
    ninja-build \
    gdb \
    lldb \
    valgrind \
    cppcheck \
    git \
    curl \
    wget \
    pkg-config \
    libssl-dev \
    liblog4cxx-dev \
    librabbitmq-dev \
    python3 \
    python3-pip \
    python3-venv \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

# Development workspace
WORKDIR /workspaces

CMD ["sleep", "infinity"]
