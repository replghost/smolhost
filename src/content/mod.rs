// SPDX-License-Identifier: GPL-3.0-or-later

//! Bounded native Bitswap retrieval. CID integrity is not chain inclusion or authorship.
//! Content is streamed without a persistent daemon cache or gateway fallback.

pub mod client;
mod dag;
pub use dag::{MAX_BLOCK_BYTES, MAX_BLOCKS, MAX_DAG_OVERHEAD_BYTES, MAX_DEPTH, MAX_FILE_BYTES};

use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, value::RawValue};
use sha2::{Digest, Sha256};
use tokio::{net::UnixStream, sync::Semaphore};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Bytes, Message},
};

use crate::engine::{Session, SharedEngine};
use dag::Walker;

pub const CHUNK_BYTES: usize = 64 * 1024;
pub const TRANSFER_TIMEOUT: Duration = Duration::from_secs(180);
const IO_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_CONTROL_BYTES: usize = 4096;
const MAX_UPSTREAM_BYTES: usize = 2 * MAX_BLOCK_BYTES + MAX_CONTROL_BYTES;
static TRANSFERS: Semaphore = Semaphore::const_new(4);

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Request {
    version: u32,
    cid: String,
    #[serde(default = "default_max_bytes")]
    max_bytes: u64,
}

fn default_max_bytes() -> u64 {
    MAX_FILE_BYTES
}

fn parse_request(text: &str) -> Result<Request> {
    ensure!(
        text.len() <= MAX_CONTROL_BYTES,
        "content request exceeds limit"
    );
    let request: Request = serde_json::from_str(text).context("invalid content request")?;
    ensure!(request.version == 1, "unsupported content protocol version");
    ensure!(
        request.max_bytes <= MAX_FILE_BYTES,
        "max_bytes exceeds content limit"
    );
    Ok(request)
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub cid: String,
    pub bytes: u64,
    pub blocks: u64,
    pub sha256: String,
}

#[derive(Deserialize)]
#[serde(tag = "event", rename_all = "lowercase", deny_unknown_fields)]
enum Control {
    Started {
        version: u32,
        cid: String,
        chain: String,
    },
    Completed {
        cid: String,
        bytes: u64,
        blocks: u64,
        sha256: String,
    },
    Error {
        message: String,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Cancel {
    event: CancelEvent,
}
#[derive(Deserialize)]
enum CancelEvent {
    #[serde(rename = "cancel")]
    Cancel,
}

pub(crate) async fn serve(
    socket: WebSocketStream<UnixStream>,
    engine: SharedEngine,
    chain: &str,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + TRANSFER_TIMEOUT;
    let (mut sender, mut receiver) = socket.split();
    let request = tokio::time::timeout(IO_TIMEOUT, async {
        loop {
            match receiver
                .next()
                .await
                .context("connection closed before content request")??
            {
                Message::Text(text) => {
                    return parse_request(&text);
                }
                Message::Ping(_) | Message::Pong(_) => (),
                _ => bail!("expected one content request"),
            }
        }
    })
    .await
    .context("content request timed out")
    .and_then(|result| result);
    let result = match request {
        Ok(request) => {
            // Dropping this entire future on cancellation drops its private Session,
            // which removes the frontend and the upstream Bitswap cancel guard.
            with_cancellation(&mut receiver, async {
                tokio::time::timeout_at(deadline, transfer(&mut sender, engine, chain, request))
                    .await
                    .context("content transfer timed out")
                    .and_then(|result| result)
            })
            .await
        }
        Err(error) => Err(error),
    };
    if let Err(error) = &result {
        let message: String = error.to_string().chars().take(512).collect();
        let _ = send(
            &mut sender,
            Message::Text(
                json!({"event":"error", "message":message})
                    .to_string()
                    .into(),
            ),
        )
        .await;
    }
    let _ = tokio::time::timeout(IO_TIMEOUT, sender.close()).await;
    result
}

/// The receive half remains polled even while upstream or the send half stalls.
async fn with_cancellation<S, F>(receiver: &mut S, work: F) -> Result<()>
where
    S: Stream<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
    F: Future<Output = Result<()>>,
{
    tokio::select! {
        biased;
        cancelled = cancellation(receiver) => cancelled,
        result = work => result,
    }
}

async fn cancellation<S>(receiver: &mut S) -> Result<()>
where
    S: Stream<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        match receiver.next().await {
            None | Some(Ok(Message::Close(_))) => return Ok(()),
            Some(Err(error)) => return Err(error.into()),
            Some(Ok(Message::Text(text))) => {
                ensure!(
                    text.len() <= MAX_CONTROL_BYTES,
                    "content control exceeds limit"
                );
                let cancel: Cancel = serde_json::from_str(&text)
                    .context("only cancellation is accepted after the request")?;
                match cancel.event {
                    CancelEvent::Cancel => return Ok(()),
                }
            }
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => (),
            _ => bail!("unexpected content control frame"),
        }
    }
}

async fn send<S>(sender: &mut S, message: Message) -> Result<()>
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    tokio::time::timeout(IO_TIMEOUT, sender.send(message))
        .await
        .context("content write timed out")??;
    Ok(())
}

async fn transfer<S>(
    sender: &mut S,
    engine: SharedEngine,
    chain: &str,
    request: Request,
) -> Result<()>
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let _permit = TRANSFERS
        .try_acquire()
        .context("all four content transfer slots are busy")?;
    let mut walker = Walker::new(&request.cid, request.max_bytes)?;
    let mut session = Session::open(engine, chain)?;
    send(
        sender,
        Message::Text(
            json!({"event":"started", "version":1, "cid":request.cid, "chain":chain})
                .to_string()
                .into(),
        ),
    )
    .await?;
    let mut digest = Sha256::new();
    while let Some(cid) = walker.next_cid() {
        let block = fetch_block(&mut session, cid).await?;
        if let Some(bytes) = walker.accept(block)? {
            digest.update(&bytes);
            let bytes = Bytes::from(bytes);
            for offset in (0..bytes.len()).step_by(CHUNK_BYTES) {
                let end = (offset + CHUNK_BYTES).min(bytes.len());
                send(sender, Message::Binary(bytes.slice(offset..end))).await?;
            }
        }
    }
    // No live upstream subscription remains: each block waits for streamDone.
    drop(session);
    send(
        sender,
        Message::Text(
            json!({
                "event":"completed", "cid":request.cid, "bytes":walker.bytes(),
                "blocks":walker.blocks(), "sha256":format!("{:x}", digest.finalize())
            })
            .to_string()
            .into(),
        ),
    )
    .await
}

#[derive(Deserialize)]
struct RpcReply<'a> {
    jsonrpc: &'a str,
    id: u64,
    #[serde(borrow)]
    result: Option<&'a RawValue>,
    #[serde(borrow)]
    error: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct Notification<'a> {
    jsonrpc: &'a str,
    method: &'a str,
    #[serde(borrow)]
    params: NotificationParams<'a>,
}
#[derive(Deserialize)]
struct NotificationParams<'a> {
    subscription: &'a str,
    #[serde(borrow)]
    result: BlockEvent<'a>,
}
#[derive(Deserialize)]
#[serde(tag = "event", rename_all = "camelCase", deny_unknown_fields)]
enum BlockEvent<'a> {
    StreamItem {
        cid: &'a str,
        value: &'a str,
    },
    StreamItemError {
        cid: &'a str,
        code: i32,
        message: &'a str,
    },
    StreamDone,
}

async fn upstream_response(session: &mut Session) -> Result<String> {
    let response = session
        .responses
        .next()
        .await
        .context("Bitswap frontend closed")?;
    ensure!(
        response.len() <= MAX_UPSTREAM_BYTES,
        "Bitswap response exceeds block limit"
    );
    Ok(response)
}

async fn fetch_block(session: &mut Session, cid: &str) -> Result<Vec<u8>> {
    // The method has ONE argument: an array of CIDs, not a flattened CID list.
    session.request(
        json!({"jsonrpc":"2.0", "id":1, "method":"bitswap_unstable_stream", "params":[[cid]]})
            .to_string(),
    )?;
    let response = upstream_response(session).await?;
    let reply: RpcReply<'_> =
        serde_json::from_str(&response).context("invalid Bitswap subscription reply")?;
    ensure!(
        reply.jsonrpc == "2.0" && reply.id == 1,
        "unexpected Bitswap reply"
    );
    if let Some(error) = reply.error {
        bail!(
            "Bitswap subscription failed: {}",
            error.get().chars().take(256).collect::<String>()
        );
    }
    let subscription: &str =
        serde_json::from_str(reply.result.context("missing Bitswap subscription")?.get())?;
    ensure!(
        !subscription.is_empty() && subscription.len() <= 256,
        "invalid Bitswap subscription ID"
    );
    let mut block = None;
    loop {
        let notification = upstream_response(session).await?;
        if accept_event(&notification, subscription, cid, &mut block)? {
            return block.context("Bitswap completed without a block");
        }
    }
}

fn accept_event(
    text: &str,
    subscription: &str,
    cid: &str,
    block: &mut Option<Vec<u8>>,
) -> Result<bool> {
    ensure!(
        text.len() <= MAX_UPSTREAM_BYTES,
        "Bitswap response exceeds block limit"
    );
    let notification: Notification<'_> =
        serde_json::from_str(text).context("invalid Bitswap event")?;
    ensure!(
        notification.jsonrpc == "2.0"
            && notification.method == "bitswap_unstable_streamEvent"
            && notification.params.subscription == subscription,
        "unexpected Bitswap notification"
    );
    match notification.params.result {
        BlockEvent::StreamItem {
            cid: received,
            value,
        } => {
            ensure!(
                received == cid && block.is_none(),
                "unexpected or repeated Bitswap block"
            );
            *block = Some(decode_block(value)?);
            Ok(false)
        }
        BlockEvent::StreamItemError {
            cid: received,
            code,
            message,
        } => {
            ensure!(received == cid, "Bitswap error has wrong CID");
            bail!(
                "Bitswap block failed ({code}): {}",
                message.chars().take(256).collect::<String>()
            );
        }
        BlockEvent::StreamDone => {
            ensure!(block.is_some(), "Bitswap completed without a block");
            Ok(true)
        }
    }
}

fn decode_block(value: &str) -> Result<Vec<u8>> {
    ensure!(
        value.len() <= 2 * MAX_BLOCK_BYTES + 2,
        "Bitswap block exceeds limit"
    );
    let hex = value
        .strip_prefix("0x")
        .context("Bitswap block is not hex")?;
    ensure!(hex.len() % 2 == 0, "Bitswap block has odd hex length");
    fn digit(byte: u8) -> Result<u8> {
        match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            b'A'..=b'F' => Ok(byte - b'A' + 10),
            _ => bail!("Bitswap block contains invalid hex"),
        }
    }
    let mut block = Vec::with_capacity(hex.len() / 2);
    for pair in hex.as_bytes().chunks_exact(2) {
        block.push((digit(pair[0])? << 4) | digit(pair[1])?);
    }
    Ok(block)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(result: serde_json::Value) -> String {
        json!({"jsonrpc":"2.0", "method":"bitswap_unstable_streamEvent", "params":{"subscription":"s", "result":result}}).to_string()
    }

    #[test]
    fn bitswap_protocol_requires_item_then_done_and_matching_identity() {
        let item = event(json!({"event":"streamItem", "cid":"c", "value":"0x0001ff"}));
        let done = event(json!({"event":"streamDone"}));
        let mut block = None;
        assert!(accept_event(&done, "s", "c", &mut block).is_err());
        assert!(accept_event(&item, "wrong", "c", &mut block).is_err());
        assert!(accept_event(&item, "s", "wrong", &mut block).is_err());
        assert!(!accept_event(&item, "s", "c", &mut block).unwrap());
        assert_eq!(block.as_deref(), Some([0, 1, 255].as_slice()));
        assert!(accept_event(&item, "s", "c", &mut block).is_err());
        assert!(accept_event(&done, "s", "c", &mut block).unwrap());
        let failure = event(
            json!({"event":"streamItemError", "cid":"c", "code":-32811, "message":"request timeout"}),
        );
        assert!(accept_event(&failure, "s", "c", &mut None).is_err());
    }

    #[test]
    fn bitswap_hex_is_bounded_before_decoding() {
        for value in ["", "00", "0x0", "0xgg", "0xé"] {
            assert!(decode_block(value).is_err());
        }
        assert!(decode_block(&format!("0x{}", "00".repeat(MAX_BLOCK_BYTES + 1))).is_err());
        assert_eq!(decode_block("0x").unwrap(), Vec::<u8>::new());
    }

    #[tokio::test]
    async fn content_cancel_and_disconnect_drop_pending_work() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        struct Guard(Arc<AtomicBool>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        for messages in [
            vec![Ok(Message::Text(r#"{"event":"cancel"}"#.into()))],
            vec![],
        ] {
            let dropped = Arc::new(AtomicBool::new(false));
            let guard = Guard(dropped.clone());
            let work = async move {
                let _guard = guard;
                std::future::pending::<Result<()>>().await
            };
            let mut receiver = futures_util::stream::iter(messages);
            with_cancellation(&mut receiver, work).await.unwrap();
            assert!(dropped.load(Ordering::SeqCst));
        }
        let mut receiver = futures_util::stream::iter(vec![Ok(Message::Text(
            r#"{"event":"cancel","extra":1}"#.into(),
        ))]);
        assert!(cancellation(&mut receiver).await.is_err());
    }

    #[test]
    fn content_request_rejects_unknown_fields_versions_and_relaxed_limits() {
        let default = parse_request(r#"{"version":1,"cid":"c"}"#).unwrap();
        assert_eq!(default.max_bytes, MAX_FILE_BYTES);
        assert_eq!(
            parse_request(r#"{"version":1,"cid":"c","max_bytes":0}"#)
                .unwrap()
                .max_bytes,
            0
        );
        for invalid in [
            r#"{"version":2,"cid":"c"}"#,
            r#"{"version":1,"cid":"c","max_bytes":67108865}"#,
            r#"{"version":1,"cid":"c","max_bytes":-1}"#,
            r#"{"version":1,"cid":"c","max_bytes":null}"#,
            r#"{"version":1,"cid":"c","output":"/tmp/file"}"#,
            r#"{"version":1,"cid":"c","version":1}"#,
            r#"{"version":1,"cid":"c"}{}"#,
            r#"[]"#,
        ] {
            assert!(parse_request(invalid).is_err(), "{invalid}");
        }
        assert!(parse_request(&" ".repeat(MAX_CONTROL_BYTES + 1)).is_err());
    }
}

#[cfg(test)]
mod backpressure_tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use tokio_tungstenite::tungstenite::protocol::Role;

    #[tokio::test]
    async fn content_cancel_interrupts_blocked_socket_writer() {
        let (server, client) = UnixStream::pair().unwrap();
        let server = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
        let mut client = WebSocketStream::from_raw_socket(client, Role::Client, None).await;
        let (mut sender, mut receiver) = server.split();
        struct Guard(Arc<AtomicBool>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let writes = Arc::new(AtomicUsize::new(0));
        let guard = Guard(dropped.clone());
        let sent = writes.clone();
        let worker = tokio::spawn(async move {
            with_cancellation(&mut receiver, async move {
                let _guard = guard;
                let chunk = Bytes::from(vec![0; CHUNK_BYTES]);
                loop {
                    send(&mut sender, Message::Binary(chunk.clone())).await?;
                    sent.fetch_add(1, Ordering::SeqCst);
                }
            })
            .await
        });
        // Never read any frames: fill the Unix send buffer, then cancel from the
        // independent receive half instead of waiting for the five-second writer.
        tokio::time::sleep(Duration::from_millis(50)).await;
        client
            .send(Message::Text(r#"{"event":"cancel"}"#.into()))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(writes.load(Ordering::SeqCst) > 0);
        assert!(dropped.load(Ordering::SeqCst));
    }
}
