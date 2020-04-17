#!/bin/bash
set -e

mkdir -p build
cd build
git clone https://github.com/GPSnoopy/vcpkg.git vcpkg.linux
cd vcpkg.linux
git checkout Arrow-0.17
./bootstrap-vcpkg.sh

./vcpkg install arrow:x64-linux
