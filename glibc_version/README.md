
`glibc_version`
===============

Crate to help rust projects discover GNU libc version at build time. Expected
to be used in `build.rs`.


Usage
-----

```rust
let ver = glibc_version::get_version().unwrap();
println!("glic version: {}.{}", ver.major, ver.minor);
```
