#!/bin/bash
set -e

mkdir -p build
cd build
git clone https://github.com/microsoft/vcpkg.git vcpkg.linux
cd vcpkg.linux
git checkout 2019.07
./bootstrap-vcpkg.sh

./vcpkg install arrow:x64-linux
