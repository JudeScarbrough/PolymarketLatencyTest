use std::time::Duration;
use tokio::time::sleep;

const INITIAL_DELAY: Duration = Duration::from_secs(1);
const MAX_DELAY: Duration = Duration::from_secs(60);

pub struct Backoff {
    delay: Duration,
}

impl Backoff {
    pub fn new() -> Self {
        Self {
            delay: INITIAL_DELAY,
        }
    }

    pub fn delay(&self) -> Duration {
        self.delay
    }

    pub async fn wait(&mut self) {
        sleep(self.delay).await;
        self.delay = (self.delay * 2).min(MAX_DELAY);
    }

    pub fn reset(&mut self) {
        self.delay = INITIAL_DELAY;
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new()
    }
}
