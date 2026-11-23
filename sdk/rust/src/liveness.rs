//! Whether the far end of a socket still answers (spec/hub-socket.md,
//! "Reconnecting", and spec/link.md, "Liveness"). Elapsed time is the
//! monotonic clock's, which stands still while the machine sleeps, so a
//! sleep spends none of a window: a wake asks afresh instead.

use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::{Instant, sleep, sleep_until};

use crate::wire::link::{DEAD_AFTER_MS, LISTENER_SILENT_MS, PING_EVERY_MS, SECOND_PING_WITHIN_MS};

const PING_EVERY: Duration = Duration::from_millis(PING_EVERY_MS);
const DEAD_AFTER: Duration = Duration::from_millis(DEAD_AFTER_MS);
const SECOND_PING_WITHIN: Duration = Duration::from_millis(SECOND_PING_WITHIN_MS);
const LISTENER_SILENT: Duration = Duration::from_millis(LISTENER_SILENT_MS);

/// What the far end has said: how many messages, and when the last came.
pub(crate) struct Heard(Mutex<(u64, Instant)>);

impl Heard {
    pub fn new() -> Heard {
        Heard(Mutex::new((0, Instant::now())))
    }

    pub fn hear(&self) {
        let mut heard = self.0.lock().unwrap();
        heard.0 += 1;
        heard.1 = Instant::now();
    }

    fn count(&self) -> u64 {
        self.0.lock().unwrap().0
    }

    fn at(&self) -> Instant {
        self.0.lock().unwrap().1
    }
}

/// Pings every 15 seconds, and returns once the far end is dead: nothing
/// arrived for 40 seconds after a ping, and a second ping then got nothing
/// back within 5. A wake sends that second ping at once.
pub(crate) async fn watch(ping: impl Fn(), heard: &Heard, mut wakes: watch::Receiver<u64>) {
    // Whether a ping sent now gets any answer within the second window.
    let probe = || async {
        let before = heard.count();
        ping();
        sleep(SECOND_PING_WITHIN).await;
        heard.count() > before
    };
    let mut pinged_at = Instant::now();
    // The first ping since the far end was last heard, and what had been
    // heard by then.
    let mut unanswered: Option<(Instant, u64)> = None;
    loop {
        let next_ping = pinged_at + PING_EVERY;
        let deadline = unanswered.map_or(next_ping, |(at, _)| at + DEAD_AFTER);
        let woke = tokio::select! {
            Ok(()) = wakes.changed() => true,
            _ = sleep_until(next_ping.min(deadline)) => false,
        };
        if unanswered.is_some_and(|(_, count)| heard.count() > count) {
            unanswered = None;
        }
        let at = Instant::now();
        if woke || unanswered.is_some_and(|_| at >= deadline) {
            if !probe().await {
                return;
            }
            unanswered = None;
            pinged_at = Instant::now();
        } else if at >= next_ping {
            ping();
            pinged_at = at;
            unanswered = unanswered.or(Some((at, heard.count())));
        }
    }
}

/// Returns once nothing was heard for 120 seconds, the listener's limit.
pub(crate) async fn silence(heard: &Heard) {
    loop {
        let quiet = heard.at().elapsed();
        if quiet >= LISTENER_SILENT {
            return;
        }
        sleep(LISTENER_SILENT - quiet).await;
    }
}
