#!/bin/sh
set -e

case $(uname) in
  Linux)
    os=linux
    triplet=x64-linux
    ;;
  Darwin)
    os=macos
    triplet=x64-osx
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
