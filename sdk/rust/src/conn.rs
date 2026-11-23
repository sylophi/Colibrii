//! One websocket, as the rest of the SDK holds it: messages in order, a
//! send and a close that act at once, and the close as it came. A task on
//! the device's runtime drives the socket itself.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, watch};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_util::task::TaskTracker;

use crate::testing::Hooks;
use crate::wire::hub::MAX_SERVICE_MESSAGE_BYTES;

/// How long a closing socket waits for the other end's close before it
/// drops the connection. A hub in workerd keeps the connection past its
/// close frame.
pub(crate) const CLOSE_GRACE: Duration = Duration::from_secs(1);

/// The code a connection that ended without a close frame reads as.
pub(crate) const CLOSE_ABNORMAL: u16 = 1006;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Msg {
    Text(String),
    Binary(Vec<u8>),
}

impl Msg {
    /// A binary message's bytes. A peer link carries nothing else.
    pub(crate) fn as_binary(&self) -> Option<&[u8]> {
        match self {
            Msg::Binary(bytes) => Some(bytes),
            Msg::Text(_) => None,
        }
    }
}

/// How a socket closed: the code and reason of the close frame that ended
/// it, sent or received.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Close {
    pub code: u16,
    pub reason: String,
}

impl From<Msg> for Message {
    fn from(message: Msg) -> Message {
        match message {
            Msg::Text(text) => Message::text(text),
            Msg::Binary(bytes) => Message::binary(bytes),
        }
    }
}

/// A close this end asked for.
#[derive(Clone)]
struct Closing {
    code: u16,
    reason: String,
    /// What was queued and not yet sent is dropped.
    cut: bool,
}

/// Whether the socket is open, and the close this end asked for, which the
/// driver acts on at once.
struct Ends {
    open: AtomicBool,
    closing: watch::Sender<Option<Closing>>,
}

impl Ends {
    fn close(&self, closing: Closing) {
        if self.open.swap(false, Ordering::SeqCst) {
            self.closing.send_replace(Some(closing));
        }
    }
}

/// What a test sees of a socket.
struct Spied {
    sent: Mutex<Vec<Msg>>,
    deaf: AtomicBool,
}

pub(crate) struct Conn {
    out: mpsc::UnboundedSender<Msg>,
    inbound: tokio::sync::Mutex<mpsc::UnboundedReceiver<Msg>>,
    closed: watch::Receiver<Option<Close>>,
    ends: Arc<Ends>,
    spied: Option<Arc<Spied>>,
}

impl Conn {
    fn start<S>(ws: WebSocketStream<S>, spy: bool, tasks: &TaskTracker) -> Conn
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (out, out_rx) = mpsc::unbounded_channel();
        let (inbound_tx, inbound) = mpsc::unbounded_channel();
        let (closed_tx, closed) = watch::channel(None);
        let ends = Arc::new(Ends { open: AtomicBool::new(true), closing: watch::channel(None).0 });
        tasks.spawn(drive(ws, out_rx, inbound_tx, closed_tx, ends.clone()));
        let spied = spy.then(|| Arc::new(Spied { sent: Mutex::new(Vec::new()), deaf: AtomicBool::new(false) }));
        Conn { out, inbound: tokio::sync::Mutex::new(inbound), closed, ends, spied }
    }

    /// A socket accepted by a listener, past its upgrade.
    pub fn accepted<S>(ws: WebSocketStream<S>, tasks: &TaskTracker) -> Conn
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Conn::start(ws, false, tasks)
    }

    /// Sends at once, while the socket is open.
    pub fn send(&self, message: Msg) {
        if self.is_open() {
            if let Some(spied) = &self.spied {
                spied.sent.lock().unwrap().push(message.clone());
            }
            let _ = self.out.send(message);
        }
    }

    /// Closes the socket after what was sent before, unless it is closing
    /// already. However the other end takes it, the socket is let go a
    /// second after.
    pub fn close(&self, code: u16, reason: &str) {
        self.ends.close(Closing { code, reason: reason.to_owned(), cut: false });
    }

    /// Closes the socket as `close` does, dropping what is still queued.
    pub fn cut(&self, code: u16, reason: &str) {
        self.ends.close(Closing { code, reason: reason.to_owned(), cut: true });
    }

    /// False once the socket is closing, by either end.
    pub fn is_open(&self) -> bool {
        self.ends.open.load(Ordering::SeqCst)
    }

    fn deaf(&self) -> bool {
        self.spied.as_ref().is_some_and(|spied| spied.deaf.load(Ordering::SeqCst))
    }

    /// The next message, in order. Once the socket closed and every message
    /// before its close was taken, the close.
    pub async fn receive(&self) -> Result<Msg, Close> {
        if let Some(message) = self.inbound.lock().await.recv().await {
            return Ok(message);
        }
        if self.deaf() {
            std::future::pending::<()>().await;
        }
        Err(self.closed().await)
    }

    pub async fn closed(&self) -> Close {
        let mut closed = self.closed.clone();
        let close = closed.wait_for(Option::is_some).await.map(|close| close.clone());
        if self.deaf() {
            std::future::pending::<()>().await;
        }
        close.ok().flatten().unwrap_or(Close { code: CLOSE_ABNORMAL, reason: String::new() })
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.close(1000, "closed");
    }
}

type Sink<S> = SplitSink<WebSocketStream<S>, Message>;

/// How reading the socket ended.
enum Read {
    Closed(Close),
    TooBig,
    Broken,
}

/// Reads until the socket's end, passing on each message while the socket
/// is open.
async fn read<S>(
    stream: &mut SplitStream<WebSocketStream<S>>,
    inbound: &mpsc::UnboundedSender<Msg>,
    ends: &Ends,
) -> Read
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let message = match stream.next().await {
            Some(Ok(Message::Text(text))) => Msg::Text(text.to_string()),
            Some(Ok(Message::Binary(bytes))) => Msg::Binary(bytes.to_vec()),
            Some(Ok(Message::Close(frame))) => {
                return Read::Closed(frame.map_or(Close { code: 1005, reason: String::new() }, |frame| Close {
                    code: u16::from(frame.code),
                    reason: frame.reason.to_string(),
                }));
            }
            Some(Ok(_)) => continue,
            Some(Err(WsError::Capacity(_))) => return Read::TooBig,
            Some(Err(_)) | None => return Read::Broken,
        };
        if ends.open.load(Ordering::SeqCst) {
            let _ = inbound.send(message);
        }
    }
}

/// Sends what is queued until this end asks to close, then the close
/// frame. None once the connection broke.
async fn write<S>(
    sink: &mut Sink<S>,
    out: &mut mpsc::UnboundedReceiver<Msg>,
    mut closing: watch::Receiver<Option<Closing>>,
) -> Option<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let message = tokio::select! {
            biased;
            _ = closing.wait_for(Option::is_some) => break,
            message = out.recv() => message,
        };
        let Some(message) = message else { break };
        sink.send(message.into()).await.ok()?;
    }
    let Closing { code, reason, cut } =
        closing.borrow().clone().unwrap_or(Closing { code: 1000, reason: "closed".into(), cut: false });
    while let Some(message) = out.try_recv().ok().filter(|_| !cut) {
        sink.send(message.into()).await.ok()?;
    }
    let frame = CloseFrame { code: CloseCode::from(code), reason: reason.into() };
    sink.send(Message::Close(Some(frame))).await.ok()
}

async fn drive<S>(
    ws: WebSocketStream<S>,
    mut out: mpsc::UnboundedReceiver<Msg>,
    inbound: mpsc::UnboundedSender<Msg>,
    closed: watch::Sender<Option<Close>>,
    ends: Arc<Ends>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut sink, mut stream) = ws.split();
    let mut closing = ends.closing.subscribe();
    let read = {
        let reading = read(&mut stream, &inbound, &ends);
        let writing = write(&mut sink, &mut out, closing.clone());
        // From the moment this end asks to close, whatever is still being
        // written.
        let deadline = async {
            let _ = closing.wait_for(Option::is_some).await;
            sleep(CLOSE_GRACE).await;
        };
        tokio::pin!(reading, writing, deadline);
        let mut written = false;
        loop {
            tokio::select! {
                read = &mut reading => break read,
                sent = &mut writing, if !written => match sent {
                    Some(()) => written = true,
                    None => break Read::Broken,
                },
                () = &mut deadline => break Read::Broken,
            }
        }
    };
    ends.open.store(false, Ordering::SeqCst);
    let ours = closing.borrow().clone().map(|Closing { code, reason, .. }| Close { code, reason });
    let close = match (ours, read) {
        (Some(ours), _) => ours,
        (None, Read::Closed(theirs)) => {
            // Their close frame is the close: the reply goes out after, as
            // the connection is let go.
            let _ = closed.send(Some(theirs));
            drop(inbound);
            let _ = timeout(CLOSE_GRACE, sink.close()).await;
            return;
        }
        (None, Read::TooBig) => {
            let frame = CloseFrame { code: CloseCode::Size, reason: "too big".into() };
            let _ = timeout(CLOSE_GRACE, sink.send(Message::Close(Some(frame)))).await;
            Close { code: 1009, reason: "too big".into() }
        }
        (None, Read::Broken) => Close { code: CLOSE_ABNORMAL, reason: String::new() },
    };
    let _ = closed.send(Some(close));
}

/// Why a socket did not open.
#[derive(Debug, Clone)]
pub(crate) struct OpenError {
    /// How long an upgrade answered 429 asks to wait.
    pub retry_after_seconds: Option<f64>,
    pub cause: String,
}

/// A websocket to `url`, once open, with no subprotocol and no
/// compression.
/// `headers` go on the upgrade request.
pub(crate) async fn open(
    url: &str,
    headers: Vec<(String, String)>,
    hooks: &Arc<dyn Hooks>,
    tasks: &TaskTracker,
) -> Result<Conn, OpenError> {
    let refused = |cause: String| OpenError { retry_after_seconds: None, cause };
    if !hooks.open(url) {
        return Err(refused("refused by the test".into()));
    }
    let mut request = url.into_client_request().map_err(|error| refused(error.to_string()))?;
    for (name, value) in headers {
        if let (Ok(name), Ok(value)) = (HeaderName::try_from(name), HeaderValue::try_from(value)) {
            request.headers_mut().insert(name, value);
        }
    }
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_SERVICE_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_SERVICE_MESSAGE_BYTES));
    match tokio_tungstenite::connect_async_with_config(request, Some(config), true).await {
        Ok((ws, _)) => {
            let conn = Conn::start(ws, true, tasks);
            hooks.opened(Opened::of(url, &conn));
            Ok(conn)
        }
        Err(WsError::Http(response)) => {
            let status = response.status().as_u16();
            let retry_after = response.headers().get("retry-after").and_then(|value| value.to_str().ok());
            Err(OpenError {
                retry_after_seconds: (status == 429).then(|| crate::hub_api::retry_after_of(retry_after)),
                cause: format!("the upgrade was answered {status}"),
            })
        }
        Err(error) => Err(refused(error.to_string())),
    }
}

/// A socket a device opened, as a test sees it.
#[derive(Clone)]
pub struct Opened {
    pub url: String,
    spied: Arc<Spied>,
    out: mpsc::UnboundedSender<Msg>,
    closed: watch::Receiver<Option<Close>>,
    ends: Arc<Ends>,
}

impl Opened {
    fn of(url: &str, conn: &Conn) -> Opened {
        Opened {
            url: url.to_owned(),
            spied: conn.spied.clone().expect("a socket a device opened is spied on"),
            out: conn.out.clone(),
            closed: conn.closed.clone(),
            ends: conn.ends.clone(),
        }
    }

    /// Everything the device sent on it.
    pub fn sent(&self) -> Vec<Msg> {
        self.spied.sent.lock().unwrap().clone()
    }

    /// Once deaf, its close never reaches the device.
    pub fn set_deaf(&self, deaf: bool) {
        self.spied.deaf.store(deaf, Ordering::SeqCst);
    }

    /// Sends as the device, without the device knowing.
    pub fn send(&self, message: Msg) {
        let _ = self.out.send(message);
    }

    pub fn close(&self, code: u16, reason: &str) {
        self.ends.close(Closing { code, reason: reason.to_owned(), cut: false });
    }

    pub async fn closed(&self) -> Close {
        let mut closed = self.closed.clone();
        closed.wait_for(Option::is_some).await.ok().and_then(|close| close.clone()).expect("a close")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fakes;

    const PIECE: usize = 64 * 1024;

    /// A socket that queued far more than its far end has read.
    async fn backed_up() -> (Conn, WebSocketStream<tokio::io::DuplexStream>, TaskTracker) {
        let tasks = TaskTracker::new();
        let (conn, far) = fakes::conn(&tasks).await;
        for _ in 0..64 {
            conn.send(Msg::Binary(vec![0; PIECE]));
        }
        tokio::task::yield_now().await;
        (conn, far, tasks)
    }

    #[tokio::test(start_paused = true)]
    async fn a_close_lets_go_of_a_peer_that_stopped_reading_a_second_after() {
        let (conn, _far, tasks) = backed_up().await;
        conn.close(1000, "closed");
        let started = tokio::time::Instant::now();
        tasks.close();
        assert!(timeout(Duration::from_secs(5), tasks.wait()).await.is_ok());
        assert_eq!(started.elapsed(), CLOSE_GRACE);
        assert_eq!(conn.closed().await, Close { code: 1000, reason: "closed".into() });
    }

    #[tokio::test]
    async fn a_cut_drops_what_was_queued() {
        let (conn, mut far, _tasks) = backed_up().await;
        conn.cut(4001, "gone");
        let mut pieces = 0;
        let close = loop {
            match far.next().await.unwrap().unwrap() {
                Message::Binary(_) => pieces += 1,
                Message::Close(frame) => break frame.unwrap(),
                _ => {}
            }
        };
        assert!(pieces < 4, "{pieces} pieces after the cut");
        assert_eq!((u16::from(close.code), close.reason.as_str()), (4001, "gone"));
    }

    #[tokio::test]
    async fn a_close_sends_what_was_queued_first() {
        let (conn, mut far, _tasks) = backed_up().await;
        conn.close(1000, "done");
        let mut pieces = 0;
        while let Message::Binary(_) = far.next().await.unwrap().unwrap() {
            pieces += 1;
        }
        assert_eq!(pieces, 64);
    }
}
