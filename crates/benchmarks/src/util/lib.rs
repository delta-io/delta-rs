

struct TestConfig {
    data: Box<Any>
}

struct Runner {
    setup: u32,
    iteration_setup: u32,
    iteration_cleanup: u32,
    cleanup: u32
}