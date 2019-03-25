#!/bin/sh
set -e

mkdir --parents build/linux
cd build/linux
cmake -D CMAKE_PREFIX_PATH=../build/vcpkg.linux/installed/x64-linux/ ../..
make -j
