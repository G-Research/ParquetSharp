#!/bin/sh
set -e

case $(uname) in
  Linux)
    os=Linux
    triplet=x64-linux
    ;;
  Darwin)
    os=macOS
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
