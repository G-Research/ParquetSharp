#!/bin/sh
set -e

case $(uname -m) in
  x86_64)
    arch=x64
    ;;
  aarch64|arm64)
    arch=arm64
    ;;
  *)
    echo "Architecture not supported"
    exit 1
    ;;
esac

case $(uname) in
  Linux)
    os=Linux
    triplet=$arch-linux
    ;;
  Darwin)
    os=macOS
    triplet=$arch-osx
    ;;
  *)
    echo "OS not supported"
    exit 1
    ;;
esac

mkdir -p build/$os
cd build/$os
cmake -D VCPKG_TARGET_TRIPLET=$triplet -D CMAKE_TOOLCHAIN_FILE=../vcpkg.$os/scripts/buildsystems/vcpkg.cmake ../..
make -j
