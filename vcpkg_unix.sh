#!/bin/bash
set -e

case $(uname) in
  Linux)
    os=linux
    triplet=x64-linux
    ;;
  Darwin)
    os=macos
    triplet=x64-osx
    if ! which brew >/dev/null || [ ! -x $(brew --prefix)/opt/bison/bin/bison ]
    then
      echo 'The version of bison provided with macOS is too old.'
      echo 'Please install a newer version with Homebrew (https://brew.sh):'
      echo '$ brew install bison'
      exit 1
    else
      export PATH="$(brew --prefix)/opt/bison/bin:$PATH"
    fi
    ;;
  *)
    echo "OS not supported"
    exit 1
    ;;
esac

# Make sure reading vcpkg_version.txt works even when it doesn't end with a newline
read -r vcpkg_url vcpkg_ref << EOF
$(cat vcpkg_version.txt)
EOF

mkdir -p build
cd build
git clone $vcpkg_url -b $vcpkg_ref vcpkg.$os
cd vcpkg.$os
./bootstrap-vcpkg.sh

./vcpkg install arrow:$triplet
