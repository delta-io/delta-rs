use chrono::{Duration, Utc};
use deltalake::vacuum::Clock;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct TestClock {
    //TODO: mutex might be overkill. Maybe just use an atomic i64..
    now: Arc<Mutex<i64>>,
}

impl Clock for TestClock {
    fn current_timestamp_millis(&self) -> i64 {
        let inner = self.now.lock().unwrap();
        return *inner;
    }
}

impl TestClock {
    pub fn new(start: i64) -> Self {
        TestClock {
            now: Arc::new(Mutex::new(start)),
        }
    }

    pub fn from_systemtime() -> Self {
        TestClock {
            now: Arc::new(Mutex::new(Utc::now().timestamp_millis())),
        }
    }

    pub fn set_timestamp(&self, timestamp: i64) {
        let mut inner = self.now.lock().unwrap();
        *inner = timestamp;
    }

    pub fn tick(&self, duration: Duration) {
        let mut inner = self.now.lock().unwrap();
        *inner = *inner + duration.num_milliseconds();
    }
}
