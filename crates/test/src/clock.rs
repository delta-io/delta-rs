use chrono::{Duration, Utc};
use deltalake_core::operations::vacuum::Clock;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct TestClock {
    now: Arc<Mutex<i64>>,
}

impl Clock for TestClock {
    fn current_timestamp_millis(&self) -> i64 {
        let inner = self.now.lock().unwrap();
        *inner
    }
}

impl TestClock {
    pub fn from_systemtime() -> Self {
        TestClock {
            now: Arc::new(Mutex::new(Utc::now().timestamp_millis())),
        }
    }

    pub fn tick(&self, duration: Duration) {
        let mut inner = self.now.lock().unwrap();
        *inner += duration.num_milliseconds();
    }
}
