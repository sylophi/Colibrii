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
    pub async fn wait_for(&mut self, test: impl FnMut(&T) -> bool) -> Option<T> {
        self.0.wait_for(test).await.ok().map(|value| value.clone())
    }
}
