//! One websocket, as the rest of the SDK holds it: messages in order, a
//! send and a close that act at once, and the close as it came. A task on
//! the device's runtime drives the socket itself.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_util::task::TaskTracker;

use crate::testing::Hooks;
use crate::wire::hub::MAX_SERVICE_MESSAGE_BYTES;

/// How long a closing socket waits for the other end's close before it
/// drops the connection. A hub in workerd keeps the connection past its
/// close frame.
const CLOSE_GRACE: Duration = Duration::from_secs(1);

/// The code a connection that ended without a close frame reads as.
pub(crate) const CLOSE_ABNORMAL: u16 = 1006;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Msg {
    Text(String),
    Binary(Vec<u8>),
}

/// How a socket closed: the code and reason of the close frame that ended
/// it, sent or received.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Close {
    pub code: u16,
    pub reason: String,
}

enum Out {
    Msg(Msg),
    Close(u16, String),
}

/// What a test sees of a socket.
struct Spied {
    sent: Mutex<Vec<Msg>>,
    deaf: AtomicBool,
}

pub(crate) struct Conn {
    out: mpsc::UnboundedSender<Out>,
    inbound: tokio::sync::Mutex<mpsc::UnboundedReceiver<Msg>>,
    closed: watch::Receiver<Option<Close>>,
    open: Arc<AtomicBool>,
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
        let open = Arc::new(AtomicBool::new(true));
        tasks.spawn(drive(ws, out_rx, inbound_tx, closed_tx, open.clone()));
        let spied = spy.then(|| Arc::new(Spied { sent: Mutex::new(Vec::new()), deaf: AtomicBool::new(false) }));
        Conn { out, inbound: tokio::sync::Mutex::new(inbound), closed, open, spied }
    }

    /// A socket accepted by a listener, past its upgrade.
    pub async fn accepted<S>(stream: S, max_message_bytes: usize, tasks: &TaskTracker) -> Conn
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let config = WebSocketConfig::default()
            .max_message_size(Some(max_message_bytes))
            .max_frame_size(Some(max_message_bytes));
        let ws = WebSocketStream::from_raw_socket(
            stream,
            tokio_tungstenite::tungstenite::protocol::Role::Server,
            Some(config),
        )
        .await;
        Conn::start(ws, false, tasks)
    }

    /// Sends at once, while the socket is open.
    pub fn send(&self, message: Msg) {
        if self.is_open() {
            if let Some(spied) = &self.spied {
                spied.sent.lock().unwrap().push(message.clone());
            }
            let _ = self.out.send(Out::Msg(message));
        }
    }

    /// Closes the socket, unless it is closing already.
    pub fn close(&self, code: u16, reason: &str) {
        if self.open.swap(false, Ordering::SeqCst) {
            let _ = self.out.send(Out::Close(code, reason.to_owned()));
        }
    }

    /// False once the socket is closing, by either end.
    pub fn is_open(&self) -> bool {
        self.open.load(Ordering::SeqCst)
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

async fn drive<S>(
    ws: WebSocketStream<S>,
    mut out: mpsc::UnboundedReceiver<Out>,
    inbound: mpsc::UnboundedSender<Msg>,
    closed: watch::Sender<Option<Close>>,
    open: Arc<AtomicBool>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut sink, mut stream) = ws.split();
    let close = loop {
        tokio::select! {
            biased;
            command = out.recv() => {
                let (code, reason) = match command {
                    Some(Out::Msg(Msg::Text(text))) => {
                        if sink.send(Message::text(text)).await.is_err() { break abnormal(&open) }
                        continue;
                    }
                    Some(Out::Msg(Msg::Binary(bytes))) => {
                        if sink.send(Message::binary(bytes)).await.is_err() { break abnormal(&open) }
                        continue;
                    }
                    Some(Out::Close(code, reason)) => (code, reason),
                    None => (1000, "closed".to_owned()),
                };
                open.store(false, Ordering::SeqCst);
                let frame = CloseFrame { code: CloseCode::from(code), reason: reason.clone().into() };
                let _ = tokio::time::timeout(CLOSE_GRACE, async {
                    if sink.send(Message::Close(Some(frame))).await.is_ok() {
                        // Until the other end's close, or its connection ends.
                        while let Some(Ok(message)) = stream.next().await {
                            if message.is_close() { break }
                        }
                    }
                }).await;
                break Close { code, reason };
            }
            frame = stream.next() => match frame {
                Some(Ok(Message::Text(text))) => { let _ = inbound.send(Msg::Text(text.to_string())); }
                Some(Ok(Message::Binary(bytes))) => { let _ = inbound.send(Msg::Binary(bytes.to_vec())); }
                Some(Ok(Message::Close(frame))) => {
                    open.store(false, Ordering::SeqCst);
                    // The close frame is the close: the reply goes out
                    // after, as the connection is let go.
                    let close = frame.map_or(Close { code: 1005, reason: String::new() }, |frame| Close {
                        code: u16::from(frame.code),
                        reason: frame.reason.to_string(),
                    });
                    let _ = closed.send(Some(close));
                    drop(inbound);
                    let _ = tokio::time::timeout(CLOSE_GRACE, sink.close()).await;
                    return;
                }
                Some(Ok(_)) => {}
                Some(Err(WsError::Capacity(_))) => {
                    open.store(false, Ordering::SeqCst);
                    let frame = CloseFrame { code: CloseCode::Size, reason: "too big".into() };
                    let _ = tokio::time::timeout(CLOSE_GRACE, sink.send(Message::Close(Some(frame)))).await;
                    break Close { code: 1009, reason: "too big".into() };
                }
                Some(Err(_)) | None => break abnormal(&open),
            },
        }
    };
    let _ = closed.send(Some(close));
}

fn abnormal(open: &AtomicBool) -> Close {
    open.store(false, Ordering::SeqCst);
    Close { code: CLOSE_ABNORMAL, reason: String::new() }
}

/// Why a socket did not open.
#[derive(Debug, Clone)]
pub(crate) struct OpenError {
    /// The HTTP status the upgrade was answered with instead.
    pub status: Option<u16>,
    pub retry_after_seconds: Option<f64>,
    pub cause: String,
}

/// A websocket to `url`, once open, with no subprotocol and no
/// compression.
pub(crate) async fn open(url: &str, hooks: &Arc<dyn Hooks>, tasks: &TaskTracker) -> Result<Conn, OpenError> {
    let refused = |cause: String| OpenError { status: None, retry_after_seconds: None, cause };
    if !hooks.open(url) {
        return Err(refused("refused by the test".into()));
    }
    let mut request = url.into_client_request().map_err(|error| refused(error.to_string()))?;
    for (name, value) in hooks.headers() {
        if let (Ok(name), Ok(value)) =
            (name.parse::<tokio_tungstenite::tungstenite::http::HeaderName>(), HeaderValue::from_str(&value))
        {
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
        Err(WsError::Http(response)) => Err(OpenError {
            status: Some(response.status().as_u16()),
            retry_after_seconds: Some(crate::hub_api::retry_after_of(
                response.headers().get("retry-after").and_then(|value| value.to_str().ok()),
            )),
            cause: format!("the upgrade was answered {}", response.status().as_u16()),
        }),
        Err(error) => Err(refused(error.to_string())),
    }
}

/// A socket a device opened, as a test sees it.
#[derive(Clone)]
pub struct Opened {
    pub url: String,
    spied: Arc<Spied>,
    out: mpsc::UnboundedSender<Out>,
    closed: watch::Receiver<Option<Close>>,
    open: Arc<AtomicBool>,
}

impl Opened {
    fn of(url: &str, conn: &Conn) -> Opened {
        Opened {
            url: url.to_owned(),
            spied: conn.spied.clone().expect("a socket a device opened is spied on"),
            out: conn.out.clone(),
            closed: conn.closed.clone(),
            open: conn.open.clone(),
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
        let _ = self.out.send(Out::Msg(message));
    }

    pub fn close(&self, code: u16, reason: &str) {
        if self.open.swap(false, Ordering::SeqCst) {
            let _ = self.out.send(Out::Close(code, reason.to_owned()));
        }
    }

    pub async fn closed(&self) -> Close {
        let mut closed = self.closed.clone();
        closed.wait_for(Option::is_some).await.ok().and_then(|close| close.clone()).expect("a close")
    }
}
