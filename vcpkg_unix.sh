#!/bin/bash
set -e

case $(uname) in
  Linux)
    os=Linux
    triplet=x64-linux
    ;;
  Darwin)
    os=macOS
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
# Clone without checking out a branch, as vcpkg_ref could be a commit SHA
git clone $vcpkg_url vcpkg.$os
cd vcpkg.$os
git checkout $vcpkg_ref
./bootstrap-vcpkg.sh -disableMetrics

./vcpkg install arrow:$triplet
