#!/bin/bash
set -e

case ${1:-$(uname -m)} in
  x86_64|x64)
    arch=x64
    ;;
  aarch64|arm64)
    arch=arm64
    ;;
  *)
    echo "Architecture not supported"
    exit 1
    ;;
esac

case $(uname) in
  Linux)
    os=linux
    ;;
  Darwin)
    os=osx
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

triplet=$arch-$os

# Make sure reading vcpkg_version.txt works even when it doesn't end with a newline
read -r vcpkg_url vcpkg_ref << EOF
$(cat vcpkg_version.txt)
EOF

mkdir -p build
cd build
# Clone without checking out a branch, as vcpkg_ref could be a commit SHA
git clone $vcpkg_url vcpkg.$triplet
cd vcpkg.$triplet
git checkout $vcpkg_ref

# Only build release configuration in CI
if [ "$GITHUB_ACTIONS" == "true" ]
then
  for triplet_file in triplets/{,community/}$triplet.cmake
  do
    [ -f "$triplet_file" ] && echo "set(VCPKG_BUILD_TYPE release)" >> "$triplet_file"
  done
fi

./bootstrap-vcpkg.sh

./vcpkg install arrow:$triplet
