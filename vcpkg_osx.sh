#!/bin/bash
set -e

if [ ! -x /usr/local/opt/bison/bin/bison ]
then
  echo 'The version of bison provided with macOS is too old'
  echo 'Please install a newer version with i.e. "brew install bison"'
  exit 1
fi

read -r vcpkg_url vcpkg_ref < vcpkg_version.txt

mkdir -p build
cd build
git clone $vcpkg_url -b $vcpkg_ref vcpkg.osx
cd vcpkg.osx
./bootstrap-vcpkg.sh

PATH="/usr/local/opt/bison/bin:$PATH" ./vcpkg install arrow:x64-osx
