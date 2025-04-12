This folder contains formal specs for various parts of delta-rs implemented using [stateright](https://github.com/stateright/stateright).

Usage
-----

To model check current specs:

```rust
cargo run --release check --worker-count 3
```

To explore problematic states with visualization UI for debugging:

```rust
cargo run --release explore --worker-count 3
```
