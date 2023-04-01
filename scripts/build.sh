#!/bin/sh
set -e
rel=`grep -o '^version\s*=\s*".*"' Cargo.toml | sed   's/version = "\(.*\)"/\1/'`
echo Building release ${rel}
cargo clean
cargo build --release
cargo build --release --target x86_64-apple-darwin
mkdir target/artifacts
cd target/artifacts
mkdir macos
mkdir macos/intel
mkdir macos/apple
cp ../release/dbdump ./macos/intel
cp ../x86_64-apple-darwin/release/dbdump ./macos/apple
zip -r release-${rel}-macos.zip .
cd ../..
echo Build Completed...

# CARGO_PKG_VERSION
