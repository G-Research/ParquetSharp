FROM mcr.microsoft.com/devcontainers/dotnet:1-8.0-bookworm-slim AS dotnet

#====================================================================

FROM dotnet AS nuget

USER vscode

# Copy our projects
COPY --chown=vscode:vscode . /tmp/build/

# Populate the nuget cache with all of our dependencies
RUN for project in /tmp/build/csharp*; do \
      dotnet restore $project; \
    done

#====================================================================

FROM dotnet AS cpp

# Install the C++ dev tools
RUN apt-get update \
 && export DEBIAN_FRONTEND=noninteractive \
 && apt-get -y install --no-install-recommends \
    bison \
    build-essential \
    cmake \
    cppcheck \
    flex \
    gdb \
    ninja-build \
    pkg-config \
    valgrind \
&& apt-get autoremove -y \
&& apt-get clean -y \
&& rm -rf /var/lib/apt/lists/*

# Set vcpkg environment variables
ENV VCPKG_ROOT=/opt/vcpkg \
    VCPKG_FORCE_SYSTEM_BINARIES=1

#====================================================================

FROM cpp AS vcpkg

USER vscode

# Copy our vcpkg manifest and ports
COPY --chown=vscode:vscode vcpkg.json /tmp/build/
COPY --chown=vscode:vscode ports /tmp/build/ports

# Install vcpkg
RUN sudo mkdir -p $VCPKG_ROOT \
 && sudo chown vscode:vscode $VCPKG_ROOT \
 && git clone https://github.com/microsoft/vcpkg.git $VCPKG_ROOT \
 && cd $VCPKG_ROOT \
 && ./bootstrap-vcpkg.sh -disableMetrics

# Populate the vcpkg binary cache with all of our dependencies
RUN cd /tmp/build \
 && $VCPKG_ROOT/vcpkg install --clean-after-build \
 && bash -c 'rm -rf $VCPKG_ROOT/{buildtrees,downloads/temp,packages}' \
 && rm -rf *

#====================================================================

FROM cpp AS devcontainer

# Copy the nuget cache
COPY --from=nuget --chown=vscode:vscode /home/vscode/.nuget/packages /home/vscode/.nuget/packages

# Copy the installed vcpkg and its binary cache
COPY --from=vcpkg --chown=vscode:vscode $VCPKG_ROOT                  $VCPKG_ROOT
COPY --from=vcpkg --chown=vscode:vscode /home/vscode/.cache/vcpkg    /home/vscode/.cache/vcpkg