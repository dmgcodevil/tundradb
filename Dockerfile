FROM ubuntu:22.04

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install basic build tools and dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    pkg-config \
    python3 \
    python3-pip \
    wget \
    curl \
    lsb-release \
    gnupg \
    ca-certificates \
    cmake \
    uuid-dev \
    && rm -rf /var/lib/apt/lists/*

# Add Arrow repository
RUN wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - \
    && wget https://apache.jfrog.io/artifactory/arrow/ubuntu/apache-arrow-apt-source-latest-$(lsb_release -cs).deb \
    && apt-get update \
    && apt-get install -y ./apache-arrow-apt-source-latest-$(lsb_release -cs).deb \
    && apt-get update \
    && apt-get install -y \
    libarrow-dev \
    libparquet-dev \
    && rm -rf /var/lib/apt/lists/* \
    && rm ./apache-arrow-apt-source-latest-$(lsb_release -cs).deb

# Install nlohmann-json and GTest
RUN apt-get update && apt-get install -y \
    libgtest-dev \
    && rm -rf /var/lib/apt/lists/*

# Install nlohmann-json from source
RUN mkdir -p /tmp/json && cd /tmp/json \
    && wget https://github.com/nlohmann/json/releases/download/v3.11.2/json.hpp \
    && mkdir -p /usr/include/nlohmann \
    && cp json.hpp /usr/include/nlohmann/ \
    && cd / && rm -rf /tmp/json

# Install Google Benchmark from source
RUN cd /tmp && \
    git clone https://github.com/google/benchmark.git && \
    cd benchmark && \
    mkdir build && \
    cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release -DBENCHMARK_ENABLE_GTEST_TESTS=OFF .. && \
    make -j && \
    make install && \
    cd / && \
    rm -rf /tmp/benchmark

# Install latest CMake
RUN wget https://github.com/Kitware/CMake/releases/download/v3.30.0/cmake-3.30.0-linux-x86_64.sh \
    -q -O /tmp/cmake-install.sh \
    && chmod u+x /tmp/cmake-install.sh \
    && mkdir -p /opt/cmake \
    && /tmp/cmake-install.sh --skip-license --prefix=/opt/cmake \
    && ln -s /opt/cmake/bin/* /usr/local/bin/ \
    && rm /tmp/cmake-install.sh

# Create a non-root user
RUN useradd -m -s /bin/bash vscode
USER vscode

# Set up the working directory
WORKDIR /workspace

# Default command
CMD ["/bin/bash"] 