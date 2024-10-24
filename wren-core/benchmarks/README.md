# Wren core benchmarks

This crate contains the benchmarks for the Wren core library based on some open source benchmarks, to help
with performance improvements of Wren core.

# Supported Benchmarks

## TPCH

Run the tpch benchmark.

This benchmarks is derived from the [TPC-H][1] version
[2.17.1]. The data and answers are generated using `tpch-gen` from
[2].

[1]: http://www.tpc.org/tpch/
[2]: https://github.com/databricks/tpch-dbgen.git,
[2.17.1]: https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.1.pdf


# Running the benchmarks

## `bench.sh`

The easiest way to run benchmarks is the [bench.sh](bench.sh)
script. Usage instructions can be found with:

```shell
# show usage
./bench.sh
```

## Comparing performance of main and a branch

```shell
git checkout main

# Gather baseline data for tpch benchmark
./benchmarks/bench.sh run tpch

# Switch to the branch the branch name is mybranch and gather data
git checkout mybranch
./benchmarks/bench.sh run tpch

# Compare results in the two branches:
./bench.sh compare main mybranch
```

This produces results like:

```shell
Comparing main and mybranch
--------------------
Benchmark tpch.json
--------------------
┏━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━┓
┃ Query        ┃    main ┃mybranch ┃    Change ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━┩
│ QQuery 1     │  4.25ms │  4.26ms │ no change │
│ QQuery 2     │ 11.25ms │ 11.68ms │ no change │
│ QQuery 3     │  5.03ms │  4.97ms │ no change │
│ QQuery 4     │  3.43ms │  3.46ms │ no change │
│ QQuery 5     │  7.39ms │  7.28ms │ no change │
│ QQuery 6     │  2.26ms │  2.26ms │ no change │
│ QQuery 7     │  8.53ms │  8.51ms │ no change │
│ QQuery 8     │  9.90ms │  9.99ms │ no change │
│ QQuery 9     │  8.56ms │  8.27ms │ no change │
│ QQuery 10    │  7.37ms │  7.63ms │ no change │
│ QQuery 11    │  7.06ms │  7.00ms │ no change │
│ QQuery 12    │  4.35ms │  4.19ms │ no change │
│ QQuery 13    │  2.93ms │  2.88ms │ no change │
│ QQuery 14    │  3.34ms │  3.33ms │ no change │
│ QQuery 15    │  6.51ms │  6.49ms │ no change │
│ QQuery 16    │  4.59ms │  4.64ms │ no change │
│ QQuery 17    │  4.00ms │  4.05ms │ no change │
│ QQuery 18    │  5.46ms │  5.47ms │ no change │
│ QQuery 19    │  5.84ms │  5.72ms │ no change │
│ QQuery 20    │  7.22ms │  7.33ms │ no change │
│ QQuery 21    │  9.35ms │  9.19ms │ no change │
│ QQuery 22    │  4.54ms │  4.33ms │ no change │
└──────────────┴─────────┴─────────┴───────────┘
┏━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┓
┃ Benchmark Summary      ┃          ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━┩
│ Total Time (main)      │ 133.16ms │
│ Total Time (mybranch)  │ 132.92ms │
│ Average Time (main)    │   6.05ms │
│ Average Time (mybranch)│   6.04ms │
│ Queries Faster         │        0 │
│ Queries Slower         │        0 │
│ Queries with No Change │       22 │
└────────────────────────┴──────────┘
```

### Running Benchmarks Manually

The `tpch` benchmark can be run with a command like this

```bash
cargo run --release --bin tpch -- benchmark --query 1 -i 10 -o result.json
```
