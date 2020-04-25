#!/bin/sh
set -e

mkdir -p build/osx
cd build/osx
cmake -D VCPKG_TARGET_TRIPLET=x64-osx -D CMAKE_TOOLCHAIN_FILE=../vcpkg.osx/scripts/buildsystems/vcpkg.cmake ../..
make -j
