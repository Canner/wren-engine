# Modeling Module

This is just a very early version of transforming SQL with DataFusion. The main program is a simple use case demonstrating how to use the mdl library.

There are some modules:

- mdl: The main entry point for using Wren modeling.
- logical_plan: The rewrite rule based on the logical planner of DataFusion.

# How to Test / Build

- Run Tset
Currently, the test cases are placed in `src/mdl/mod.rs`
```
cargo test
```
