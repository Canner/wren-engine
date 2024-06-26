# Wren Modeling Core in Python
Here is a dependency package for Python. It is a wrapper for the Rust package [wren-modeling-rs](../wren-modeling-rs). The Rust package is compiled to a Python package and can be used in Python.

## Developer Guide

### Environment Setup
- Install [Rust](https://www.rust-lang.org/tools/install) and [Cargo](https://doc.rust-lang.org/cargo/getting-started/installation.html)
- Install [Python](https://www.python.org/downloads/) and [pipx](https://pipx.pypa.io/)
- Install [poetry](https://github.com/python-poetry/poetry)
- Install [casey/just](https://github.com/casey/just)

### Test and build
- Execute `just install` to create python venv and install dependencies.
- Execute `just test` to test Rust and Python.
- Execute `just build` to build the Python package. You can find the wheel in the `target/wheels/` directory.

### Coding Style
Format with rustfmt via `cargo fmt`
