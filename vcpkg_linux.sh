#!/bin/bash
set -e

mkdir -p build
cd build
git clone https://github.com/philjdf/vcpkg.git vcpkg.linux
cd vcpkg.linux
git checkout Arrow014
./bootstrap-vcpkg.sh

./vcpkg install arrow:x64-linux
