FROM --platform=linux/amd64 ubuntu:24.04

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install basic build tools and dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc-13 \
    g++-13 \
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
    libboost-all-dev \
    libtbb-dev \
    libgtest-dev \
    libbenchmark-dev \
    libcds-dev \
    openjdk-11-jdk \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Set GCC 13 as default
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-13 100 \
    && update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-13 100

# Add Arrow repository
RUN wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - \
    && wget https://apache.jfrog.io/artifactory/arrow/ubuntu/apache-arrow-apt-source-latest-$(lsb_release -cs).deb \
    && apt-get update \
    && apt-get install -y ./apache-arrow-apt-source-latest-$(lsb_release -cs).deb \
    && apt-get update \
    && apt-get install -y \
    libarrow-dev \
    libarrow-dataset-dev \
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

# Install fmt library from source (latest version with fmt::join)
RUN cd /tmp && \
    git clone https://github.com/fmtlib/fmt.git && \
    cd fmt && \
    mkdir build && \
    cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release -DFMT_DOC=OFF -DFMT_TEST=OFF .. && \
    make -j$(nproc) && \
    make install && \
    cd / && \
    rm -rf /tmp/fmt

# Install latest CMake
RUN wget https://github.com/Kitware/CMake/releases/download/v3.30.0/cmake-3.30.0-linux-x86_64.sh \
    -q -O /tmp/cmake-install.sh \
    && chmod u+x /tmp/cmake-install.sh \
    && mkdir -p /opt/cmake \
    && /tmp/cmake-install.sh --skip-license --prefix=/opt/cmake \
    && ln -s /opt/cmake/bin/* /usr/local/bin/ \
    && rm /tmp/cmake-install.sh

# Install ANTLR4 runtime from source
RUN cd /tmp && \
    git clone https://github.com/antlr/antlr4.git && \
    cd antlr4/runtime/Cpp && \
    mkdir build && \
    cd build && \
    cmake .. -DCMAKE_BUILD_TYPE=Release && \
    make -j$(nproc) && \
    make install && \
    cd / && \
    rm -rf /tmp/antlr4

# Create a non-root user and add to sudo group
RUN useradd -m -s /bin/bash vscode && \
    echo "vscode ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers
USER vscode

# Set up the working directory
WORKDIR /workspace

# Force cache invalidation - updated 2025-01-26
# Copy source code
COPY . .

# Build the project
RUN sudo rm -rf build && \
    mkdir build && \
    cd build && \
    cmake .. && \
    make -j$(nproc)

# Default command
CMD ["./build/tundradb"] 