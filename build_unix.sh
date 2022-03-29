#!/bin/sh
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
    options="-D CMAKE_SYSTEM_PROCESSOR=$linux_arch \
             -D CMAKE_C_COMPILER=$(which $linux_arch-linux-gnu-gcc) \
             -D CMAKE_CXX_COMPILER=$(which $linux_arch-linux-gnu-g++) \
             -D CMAKE_STRIP=$(which $linux_arch-linux-gnu-strip)"
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

# Only build release configuration in CI
if [ "$GITHUB_ACTIONS" == "true" ]
then
  mkdir -p custom-triplets
  for triplet_file in {,community/}$triplet.cmake
  do
    vcpkg_triplet_file="$VCPKG_INSTALLATION_ROOT/triplets/$triplet_file"
    if [ -f "$vcpkg_triplet_file" ]; then
        custom_triplet_file="custom-triplets/$triplet.cmake"
        cp "$vcpkg_triplet_file" "$custom_triplet_file"
        echo "set(VCPKG_BUILD_TYPE release)" >> "$custom_triplet_file"
    fi
  done
  options="$options -D VCPKG_OVERLAY_TRIPLETS=$PWD/custom-triplets"
fi

cmake -B build/$triplet -S . -D VCPKG_TARGET_TRIPLET=$triplet -D CMAKE_TOOLCHAIN_FILE=$VCPKG_INSTALLATION_ROOT/scripts/buildsystems/vcpkg.cmake $options
cmake --build build/$triplet -j
