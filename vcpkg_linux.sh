#!/bin/bash
set -e

read -r vcpkg_url vcpkg_ref < vcpkg_version.txt

mkdir -p build
cd build
git clone $vcpkg_url -b $vcpkg_ref vcpkg.linux
cd vcpkg.linux
[ "$GITHUB_ACTIONS" == "true" ] && echo "set(VCPKG_BUILD_TYPE release)" >> triplets/x64-linux.cmake
./bootstrap-vcpkg.sh

./vcpkg install arrow:x64-linux
