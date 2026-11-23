//! Where the device's work runs: a tokio runtime on a thread of the
//! device's own, or one the app hands in. What the app holds only waits on
//! that work, so it works from any executor and from plain threads.

use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;

use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub(crate) struct Runtime {
    handle: Handle,
    // The device's own thread, ended once the device closed.
    owned: Mutex<Option<(oneshot::Sender<()>, thread::JoinHandle<()>)>>,
}

impl Runtime {
    pub fn new(given: Option<Handle>) -> std::io::Result<Runtime> {
        if let Some(handle) = given {
            return Ok(Runtime { handle, owned: Mutex::new(None) });
        }
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        let handle = runtime.handle().clone();
        let (stop, stopped) = oneshot::channel::<()>();
        let thread = thread::Builder::new().name("colibrii".into()).spawn(move || {
            runtime.block_on(async {
                let _ = stopped.await;
            });
        })?;
        Ok(Runtime { handle, owned: Mutex::new(Some((stop, thread))) })
    }

    /// Runs `future` on the device's runtime, from wherever it is awaited.
    /// Dropping what this returns stops it.
    pub fn run<F>(&self, future: F) -> Task<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        Task(self.handle.spawn(future))
    }

    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }

    /// Ends the device's own thread, whose tasks are then dropped. Joins it
    /// unless called on it.
    pub fn stop(&self) {
        if let Some((stop, thread)) = self.owned.lock().unwrap().take() {
            let _ = stop.send(());
            if thread.thread().id() != thread::current().id() {
                let _ = thread.join();
            }
        }
    }
}

/// A task on the device's runtime that stops when dropped.
pub(crate) struct Task<T>(JoinHandle<T>);

impl<T> Future for Task<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        Pin::new(&mut self.0).poll(cx).map(|result| match result {
            Ok(value) => value,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            // Only the runtime shutting down cancels a task that is awaited.
            Err(_) => panic!("the device's runtime ended"),
        })
    }
}

impl<T> Drop for Task<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Runs `future` on a runtime of its own, from wherever it is awaited, for
/// what the SDK does apart from any device.
pub(crate) async fn on_own_runtime<F>(future: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let runtime = Runtime::new(None).expect("a runtime");
    let output = runtime.run(future).await;
    runtime.stop();
    output
}

/// Runs a future to its end on this thread, for an app that calls the SDK
/// from a plain thread. Any executor does as well.
pub fn block_on<F: Future>(future: F) -> F::Output {
    struct Unpark(thread::Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return value;
        }
        thread::park();
    }
}
