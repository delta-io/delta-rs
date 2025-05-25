mod factories;

pub use factories::*;

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + 'static>>;
