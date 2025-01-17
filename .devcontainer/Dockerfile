FROM mcr.microsoft.com/devcontainers/python:3.9

# Install system packages
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
        gcc \
        git \
        make \
        curl \
        jq \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

# Install development tools
RUN pip install --no-cache-dir \
    black \
    pylint \
    pytest \
    pytest-cov \
    ipython \
    jupyter

# Create non-root user
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && apt-get update \
    && apt-get install -y sudo \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

# Set working directory
WORKDIR /workspace

# Switch to non-root user
USER $USERNAME
