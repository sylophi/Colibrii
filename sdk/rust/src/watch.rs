use tokio::sync::watch;

/// A value the device keeps, now and as it changes. It works from any
/// executor, and ends once the device is closed.
#[derive(Debug, Clone)]
pub struct Watch<T>(pub(crate) watch::Receiver<T>);

impl<T: Clone> Watch<T> {
    /// The value now.
    pub fn get(&self) -> T {
        self.0.borrow().clone()
    }

    /// The next value, once it changes from the last one this saw. Nothing
    /// once the device is closed.
    pub async fn changed(&mut self) -> Option<T> {
        self.0.changed().await.ok()?;
        Some(self.0.borrow_and_update().clone())
    }

    /// The value now if it passes `test`, or the first change that does.
    /// `test` sees a copy, so it may change the value itself, as through
    /// [`crate::Device::wake`].
    pub async fn wait_for(&mut self, mut test: impl FnMut(&T) -> bool) -> Option<T> {
        loop {
            let now = self.0.borrow_and_update().clone();
            if test(&now) {
                return Some(now);
            }
            self.0.changed().await.ok()?;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use super::*;

    #[test]
    fn a_test_may_change_the_value_it_waits_on() {
        let (done, finished) = mpsc::channel();
        std::thread::spawn(move || {
            let (count, counted) = watch::channel(0);
            let mut watching = Watch(counted);
            let passed = crate::block_on(watching.wait_for(|now| {
                if *now == 0 {
                    count.send_replace(1);
                }
                *now == 1
            }));
            let _ = done.send(passed);
        });
        assert_eq!(finished.recv_timeout(Duration::from_secs(5)), Ok(Some(1)));
    }
}
