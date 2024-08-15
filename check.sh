#!/usr/bin/env bash
set -eux

cargo +stable install cargo-hack --locked

cargo hack check --all
cargo fmt --all -- --check
cargo hack clippy --workspace --all-targets -- -D warnings -W clippy::all
cargo hack test --workspace
