# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Build & Check
```bash
cargo build
cargo check --no-default-features
cargo check --all-features
```

### Test
```bash
cargo +stable nextest run --no-default-features
cargo +stable nextest run --all-features
cargo +nightly careful test --all-features   # UB/safety checks

# Single test
cargo nextest run --all-features -E 'test(test_name)'
```

### Lint & Format
```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

### Python (crates/mate)
```bash
maturin develop --release    # Build and install Python extension
pytest tests                 # Run Python tests
```

## Architecture

This is a Rust workspace of graph algorithm crates with a layered dependency structure:

```
graph_builder  →  graph (algos)  →  graph_app (CLI)
                                 →  graph_mate (Python via PyO3)
                                 →  graph_server (Arrow Flight gRPC)
```

### crates/builder (`graph_builder`)
Foundation layer. Implements two graph representations:
- **CSR (Compressed-Sparse-Row)**: Immutable, cache-friendly, fully thread-safe — preferred for algorithms
- **AL (Adjacency List)**: Mutable, Mutex-protected — used when post-build modifications are needed

Supports generic node ID types (`usize`, `u32`, etc.) and generic edge value types for weighted graphs. Input formats: edge lists, Graph500, GDL (optional feature).

### crates/algos (`graph`)
Algorithm implementations on top of `graph_builder`. Algorithms: Page Rank, Triangle Count, SSSP (Dijkstra), WCC, Afforest, DSS. Uses rayon for parallelization. Optional features: `serde`, `clap`.

### crates/mate (`graph_mate`)
Python bindings via PyO3/maturin. Publishes to PyPI (not crates.io). Graph creation from numpy arrays, GIL-releasing algorithm execution. Uses stable ABI (`abi3`) targeting Python 3.8+.

### crates/server (`graph_server`)
Arrow Flight gRPC server for distributed clients. Manages graphs in-memory, streams results as Arrow record batches. Not published to crates.io.

### crates/app (`graph_app`)
CLI binary wrapping `graph` algorithms. Not published.

## Release Process

Releases are managed via `cargo-release` and triggered through GitHub Actions (`create-release-pr.yml`). Run the workflow manually with the crate name and version bump level; it creates a signed release PR. Published crates: `graph` and `graph_builder` on crates.io; `graph_mate` on PyPI.
