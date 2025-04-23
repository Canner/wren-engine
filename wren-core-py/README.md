# Wren Core Python binding

This is a python binding for [wren-core](../wren-core). It uses [PyO3](https://github.com/PyO3/pyo3) to build the required wheel for [ibis-server](../ibis-server/).

## Developer Guide

### Environment Setup

- Install [Rust](https://www.rust-lang.org/tools/install) and [Cargo](https://doc.rust-lang.org/cargo/getting-started/installation.html)
- Install [Python](https://www.python.org/downloads/) and [pipx](https://pipx.pypa.io/)
- Install [poetry](https://github.com/python-poetry/poetry)
- Install [casey/just](https://github.com/casey/just)

### Test and build
After install `casey/just`, you can use the following command to build or test:
- Execute `just install` to create Python venv and install dependencies.
- **Important**: Before testing Python, you need to build the Rust package by running `just develop`.
- Use `just test-rs` to test Rust only, and `just test-py` to test Python only.
- Use `just test` to test Rust and Python.
- Execute `just build` to build the Python package. You can find the wheel in the `target/wheels/` directory.

### Coding Style

Format via `just format`
