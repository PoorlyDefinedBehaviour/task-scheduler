use chrono::{DateTime, Utc};

use crate::contracts;

pub struct DefaultClock {}

impl DefaultClock {
    pub fn new() -> Self {
        Self {}
    }
}

impl contracts::Clock for DefaultClock {
    #[tracing::instrument(skip_all, ret)]
    fn utc_now(&self) -> DateTime<Utc> {
        chrono::Utc::now()
    }
}
