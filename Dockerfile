FROM ubuntu:22.04

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install basic build tools and dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    pkg-config \
    python3 \
    python3-pip \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install Arrow and Parquet dependencies
RUN apt-get update && apt-get install -y \
    libarrow-dev \
    libparquet-dev \
    && rm -rf /var/lib/apt/lists/*

# Install nlohmann-json
RUN apt-get update && apt-get install -y \
    nlohmann-json3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Google Test
RUN apt-get update && apt-get install -y \
    libgtest-dev \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN useradd -m -s /bin/bash vscode
USER vscode

# Set up the working directory
WORKDIR /workspace

# Default command
CMD ["/bin/bash"] 