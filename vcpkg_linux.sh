#!/bin/bash
set -e

mkdir -p build
cd build
git clone https://github.com/Microsoft/vcpkg.git vcpkg.linux
cd vcpkg.linux
git checkout master
./bootstrap-vcpkg.sh

./vcpkg install arrow:x64-linux
