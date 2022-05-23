#!/bin/bash
set -e

case ${1:-$(uname -m)} in
  x86_64|x64)
    vcpkg_arch=x64
    linux_arch=x86_64
    osx_arch=x86_64
    ;;
  aarch64|arm64)
    vcpkg_arch=arm64
    linux_arch=aarch64
    osx_arch=arm64
    ;;
  *)
    echo "Architecture not supported"
    exit 1
    ;;
esac

case $(uname) in
  Linux)
    os=linux
    [ -f /etc/redhat-release ] && platform=redhat-linux || platform=linux-gnu
    options="-D CMAKE_SYSTEM_PROCESSOR=$linux_arch \
             -D CMAKE_C_COMPILER=$(which $linux_arch-$platform-gcc) \
             -D CMAKE_CXX_COMPILER=$(which $linux_arch-$platform-g++) \
             -D CMAKE_STRIP=$(which $linux_arch-$platform-strip 2>/dev/null || which strip)"
    ;;
  Darwin)
    os=osx
    options="-D CMAKE_OSX_ARCHITECTURES=$osx_arch"
    if ! which brew >/dev/null || [ ! -x $(brew --prefix)/opt/bison/bin/bison ]
    then
      echo 'The version of bison provided with macOS is too old.'
      echo 'Please install a newer version with Homebrew (https://brew.sh):'
      echo '$ brew install bison'
      exit 1
    else
      export PATH="$(brew --prefix)/opt/bison/bin:$PATH"
    fi
    ;;
  *)
    echo "OS not supported"
    exit 1
    ;;
esac

triplet=$vcpkg_arch-$os

# Find vcpkg or download it if required
if [ -z "$VCPKG_INSTALLATION_ROOT" ]; then
  if [ -n "$VCPKG_ROOT" ]; then
    VCPKG_INSTALLATION_ROOT=$VCPKG_ROOT
  else
    VCPKG_INSTALLATION_ROOT=$PWD/build/vcpkg
    if [ ! -d "$VCPKG_INSTALLATION_ROOT" ]; then
        git clone https://github.com/microsoft/vcpkg.git "$VCPKG_INSTALLATION_ROOT"
        $VCPKG_INSTALLATION_ROOT/bootstrap-vcpkg.sh
    fi
  fi
fi

# Only build release configuration in CI
if [ "$GITHUB_ACTIONS" = "true" ]
then
  custom_triplets_dir=$PWD/build/custom-triplets
  mkdir -p "$custom_triplets_dir"
  for vcpkg_triplet_file in $VCPKG_INSTALLATION_ROOT/triplets/{,community/}$triplet.cmake
  do
    if [ -f "$vcpkg_triplet_file" ]; then
        custom_triplet_file="$custom_triplets_dir/$triplet.cmake"
        cp "$vcpkg_triplet_file" "$custom_triplet_file"
        echo "set(VCPKG_BUILD_TYPE release)" >> "$custom_triplet_file"
    fi
  done
  options+=" -D VCPKG_OVERLAY_TRIPLETS=$custom_triplets_dir"
fi

cmake -B build/$triplet -S . -D VCPKG_TARGET_TRIPLET=$triplet -D CMAKE_TOOLCHAIN_FILE=$VCPKG_INSTALLATION_ROOT/scripts/buildsystems/vcpkg.cmake $options
cmake --build build/$triplet -j
