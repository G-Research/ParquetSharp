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
    ;;
  *)
    echo "OS not supported"
    exit 1
    ;;
esac

triplet=$vcpkg_arch-$os

cmake -B build/$triplet -S . -D VCPKG_TARGET_TRIPLET=$triplet -D CMAKE_TOOLCHAIN_FILE=../vcpkg.$triplet/scripts/buildsystems/vcpkg.cmake $options
cmake --build build/$triplet -j
