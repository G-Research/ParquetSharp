#!/bin/sh
set -e

mkdir build
cd build
git clone https://github.com/philjdf/vcpkg.git vcpkg.linux
cd vcpkg.linux
git checkout ArrowUpdateLinux
./bootstrap-vcpkg.sh

./vcpkg install boost-format:x64-linux
./vcpkg install arrow:x64-linux
