`glibc_version`
===============

[![cratesio](https://img.shields.io/crates/v/glibc_version.svg?style=flat-square)](https://crates.io/crates/glibc_version)
[![doc](https://img.shields.io/badge/docs-api-blue)](https://docs.rs/glibc_version)

Crate to help rust projects discover GNU libc version at build time. Expected
to be used in `build.rs`.


Usage
-----

```rust
let ver = glibc_version::get_version().unwrap();
println!("glic version: {}.{}", ver.major, ver.minor);
```
