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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    /// The far end, answering each ping while `answers` holds.
    struct Far {
        heard: Heard,
        pings: AtomicU32,
        answers: std::sync::atomic::AtomicBool,
    }

    impl Far {
        fn new(answers: bool) -> Far {
            Far { heard: Heard::new(), pings: AtomicU32::new(0), answers: answers.into() }
        }

        fn ping(&self) {
            self.pings.fetch_add(1, Ordering::SeqCst);
            if self.answers.load(Ordering::SeqCst) {
                self.heard.hear();
            }
        }
    }

    /// When `watch` returned, in whole seconds, if within `limit`.
    async fn dead_after(far: &Far, wakes: watch::Receiver<u64>, limit: u64) -> Option<u64> {
        let started = Instant::now();
        let watched = tokio::time::timeout(Duration::from_secs(limit), watch(|| far.ping(), &far.heard, wakes));
        watched.await.ok().map(|()| started.elapsed().as_secs())
    }

    #[tokio::test(start_paused = true)]
    async fn pings_every_15_seconds_while_the_far_end_answers() {
        let far = Far::new(true);
        let wakes = watch::channel(0);
        assert_eq!(dead_after(&far, wakes.1, 61).await, None);
        assert_eq!(far.pings.load(Ordering::SeqCst), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn is_dead_40_seconds_after_an_unanswered_ping_and_5_more_after_a_second() {
        let far = Far::new(false);
        let wakes = watch::channel(0);
        // The first ping at 15, the second at 55, nothing back by 60.
        assert_eq!(dead_after(&far, wakes.1, 120).await, Some(60));
        assert_eq!(far.pings.load(Ordering::SeqCst), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn lives_on_while_anything_arrives_after_an_unanswered_ping() {
        let far = Far::new(false);
        let wakes = watch::channel(0);
        let chatter = async {
            for _ in 0..12 {
                sleep(Duration::from_secs(10)).await;
                far.heard.hear();
            }
        };
        let (dead, ()) = tokio::join!(dead_after(&far, wakes.1, 121), chatter);
        assert_eq!(dead, None);
    }

    #[tokio::test(start_paused = true)]
    async fn pings_at_once_on_a_wake_and_is_dead_when_that_goes_unanswered() {
        let far = Far::new(false);
        let wakes = watch::channel(0);
        let waking = async {
            sleep(Duration::from_secs(2)).await;
            wakes.0.send_modify(|count| *count += 1);
        };
        let (dead, ()) = tokio::join!(dead_after(&far, wakes.1.clone(), 120), waking);
        assert_eq!(dead, Some(7));
        assert_eq!(far.pings.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn lives_on_after_a_wake_whose_ping_is_answered() {
        let far = Far::new(true);
        let wakes = watch::channel(0);
        let waking = async {
            sleep(Duration::from_secs(2)).await;
            wakes.0.send_modify(|count| *count += 1);
        };
        let (dead, ()) = tokio::join!(dead_after(&far, wakes.1.clone(), 30), waking);
        assert_eq!(dead, None);
        // The wake's at 2, then every 15 seconds from its answer at 7.
        assert_eq!(far.pings.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn the_listeners_limit_is_120_seconds_of_silence() {
        let heard = Heard::new();
        let started = Instant::now();
        let talking = async {
            sleep(Duration::from_secs(100)).await;
            heard.hear();
        };
        tokio::join!(silence(&heard), talking);
        assert_eq!(started.elapsed().as_secs(), 220);
    }
}
