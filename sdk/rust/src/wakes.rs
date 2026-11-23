//! A wake as a process can tell one: the monotonic clock stands still while
//! the machine sleeps and the wall clock runs on, so a turn after which the
//! wall clock moved further than the monotonic one came after a sleep. A
//! process gets no word of the network going or coming back, which the app
//! passes on through `Device::wake`.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use crate::device::Inner;

const CHECK_EVERY: Duration = Duration::from_secs(2);
// More than a late timer, less than any sleep worth a reconnect.
const SLEPT: Duration = Duration::from_secs(5);

pub(crate) async fn watch_clocks(inner: Arc<Inner>) {
    let mut last = (SystemTime::now(), Instant::now());
    loop {
        tokio::select! {
            _ = inner.closing.cancelled() => return,
            _ = tokio::time::sleep(CHECK_EVERY) => {}
        }
        let now = (SystemTime::now(), Instant::now());
        let wall = now.0.duration_since(last.0).unwrap_or_default();
        if wall.saturating_sub(now.1 - last.1) > SLEPT {
            inner.wake();
        }
        last = now;
    }
}
