// SPDX-License-Identifier: GPL-3.0-or-later

//! Bounded native Bitswap retrieval. CID integrity is not chain inclusion or authorship.
//! Content is streamed without a persistent daemon cache or gateway fallback.

pub mod client;
mod dag;
pub use dag::{MAX_BLOCK_BYTES, MAX_BLOCKS, MAX_DAG_OVERHEAD_BYTES, MAX_DEPTH, MAX_FILE_BYTES};

use std::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, value::RawValue};
use sha2::{Digest, Sha256};
use tokio::{net::UnixStream, sync::Semaphore, time::Instant};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Bytes, Message},
};

use crate::{
    discovery::{ContentCapabilities, ContentDeadlines, ContentEndpoint, ContentLimits},
    engine::{Session, SharedEngine},
};
use dag::Walker;

pub const PROTOCOL_VERSION: u32 = 1;
pub(crate) const ROUTE_PREFIX: &str = "/content/";
pub const CHUNK_BYTES: usize = 64 * 1024;
pub const TRANSFER_TIMEOUT: Duration = Duration::from_secs(180);
const IO_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_CONTROL_BYTES: usize = 4096;
const MAX_UPSTREAM_BYTES: usize = 2 * MAX_BLOCK_BYTES + MAX_CONTROL_BYTES;
const MAX_CONCURRENT_TRANSFERS: usize = 4;
static TRANSFERS: Semaphore = Semaphore::const_new(MAX_CONCURRENT_TRANSFERS);
static NEXT_TRANSFER: AtomicU64 = AtomicU64::new(1);

// Retained outside the cancellable work so deadline/disconnect diagnostics keep
// the last observed stage. No payloads or CIDs are retained here.
struct Progress {
    id: u64,
    began: Instant,
    phase: &'static str,
    phase_began: Instant,
    block_began: Option<Instant>,
    block: u64,
    verified_blocks: u64,
    verified_bytes: u64,
    sent_bytes: u64,
}

impl Progress {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            id: NEXT_TRANSFER.fetch_add(1, Ordering::Relaxed),
            began: now,
            phase: "await_request",
            phase_began: now,
            block_began: None,
            block: 0,
            verified_blocks: 0,
            verified_bytes: 0,
            sent_bytes: 0,
        }
    }

    fn phase(&mut self, phase: &'static str) {
        self.phase = phase;
        self.phase_began = Instant::now();
    }

    fn begin_block(&mut self) {
        self.block += 1;
        self.phase("enqueue_block");
        self.block_began = Some(self.phase_began);
    }

    fn error(&self, chain: &str, error: anyhow::Error) -> anyhow::Error {
        error.context(format!("content chain={chain} {self}"))
    }
}

impl fmt::Display for Progress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let now = Instant::now();
        write!(
            f,
            "transfer={} phase={} elapsed_ms={} phase_ms={} block={} block_ms={} verified_blocks={} verified_bytes={} sent_bytes={}",
            self.id,
            self.phase,
            (now - self.began).as_millis(),
            (now - self.phase_began).as_millis(),
            self.block,
            self.block_began
                .map_or(0, |began| (now - began).as_millis()),
            self.verified_blocks,
            self.verified_bytes,
            self.sent_bytes,
        )
    }
}

pub(crate) fn capabilities(chains: &[String]) -> ContentCapabilities {
    ContentCapabilities {
        protocol_version: PROTOCOL_VERSION,
        endpoints: chains
            .iter()
            .map(|chain| ContentEndpoint {
                chain_id: chain.clone(),
                path: format!("{ROUTE_PREFIX}{chain}"),
            })
            .collect(),
        cid_versions: vec![1],
        multibases: vec!["base32lower".into()],
        codecs: vec![0x55, 0x70],
        hashes: vec![0x12, 0x1b, 0xb220],
        unixfs_types: vec!["file".into(), "raw".into()],
        limits: ContentLimits {
            max_file_bytes: MAX_FILE_BYTES,
            max_block_bytes: MAX_BLOCK_BYTES as u64,
            max_blocks: MAX_BLOCKS as u64,
            max_depth: MAX_DEPTH as u64,
            max_dag_overhead_bytes: MAX_DAG_OVERHEAD_BYTES,
            max_chunk_bytes: CHUNK_BYTES as u64,
            max_control_bytes: MAX_CONTROL_BYTES as u64,
            max_concurrent_transfers: MAX_CONCURRENT_TRANSFERS as u64,
            max_connections: crate::ipc::MAX_CONNECTIONS as u64,
        },
        deadlines: ContentDeadlines {
            handshake_ms: crate::ipc::IO_TIMEOUT.as_millis() as u64,
            request_ms: IO_TIMEOUT.as_millis() as u64,
            write_ms: IO_TIMEOUT.as_millis() as u64,
            transfer_ms: TRANSFER_TIMEOUT.as_millis() as u64,
        },
    }
}

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
    ensure!(
        request.version == PROTOCOL_VERSION,
        "unsupported content protocol version"
    );
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
    let mut progress = Progress::new();
    let (mut sender, mut receiver) = socket.split();
    let request = tokio::time::timeout(IO_TIMEOUT, async {
        loop {
            match receiver
                .next()
                .await
                .context("connection closed before content request")??
            {
                Message::Text(text) => {
                    progress.phase("validate_request");
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
                tokio::time::timeout_at(
                    deadline,
                    transfer(&mut sender, engine, chain, request, &mut progress),
                )
                .await
                .context("content transfer timed out")
                .and_then(|result| result)
            })
            .await
        }
        Err(error) => Err(error),
    };
    let outcome = match (&result, progress.phase) {
        (Err(_), _) => "failed",
        (Ok(()), "completed") => "completed",
        (Ok(()), _) => "cancelled",
    };
    log::debug!("host: content ended chain={chain} outcome={outcome} {progress}");
    let result = result.map_err(|error| progress.error(chain, error));
    if let Err(error) = &result {
        let message: String = format!("{error:#}").chars().take(512).collect();
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
    progress: &mut Progress,
) -> Result<()>
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    progress.phase("acquire_slot");
    let _permit = TRANSFERS
        .try_acquire()
        .context("all content transfer slots are busy")?;
    progress.phase("validate_cid");
    let mut walker = Walker::new(&request.cid, request.max_bytes)?;
    progress.phase("open_session");
    let mut session = Session::open(engine, chain)?;
    progress.phase("write_started");
    send(
        sender,
        Message::Text(
            json!({"event":"started", "version":PROTOCOL_VERSION, "cid":request.cid, "chain":chain})
                .to_string()
                .into(),
        ),
    )
    .await?;
    let mut digest = Sha256::new();
    while let Some(cid) = walker.next_cid() {
        progress.begin_block();
        log::debug!(
            "host: content block_start chain={chain} transfer={} block={} cid={cid}",
            progress.id,
            progress.block,
        );
        let block = fetch_block(&mut session, cid, progress).await?;
        progress.phase("verify_block");
        let bytes = walker.accept(block)?;
        progress.verified_blocks = walker.blocks();
        progress.verified_bytes = walker.bytes();
        if let Some(bytes) = bytes {
            digest.update(&bytes);
            let bytes = Bytes::from(bytes);
            for offset in (0..bytes.len()).step_by(CHUNK_BYTES) {
                let end = (offset + CHUNK_BYTES).min(bytes.len());
                progress.phase("write_chunk");
                send(sender, Message::Binary(bytes.slice(offset..end))).await?;
                progress.sent_bytes += (end - offset) as u64;
            }
        }
    }
    // No live upstream subscription remains: each block waits for streamDone.
    drop(session);
    progress.phase("write_completed");
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
    .await?;
    progress.phase("completed");
    Ok(())
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

async fn upstream_response(responses: &mut (impl Stream<Item = String> + Unpin)) -> Result<String> {
    let response = responses.next().await.context("Bitswap frontend closed")?;
    ensure!(
        response.len() <= MAX_UPSTREAM_BYTES,
        "Bitswap response exceeds block limit"
    );
    Ok(response)
}

async fn fetch_block(session: &mut Session, cid: &str, progress: &mut Progress) -> Result<Vec<u8>> {
    // The method has ONE argument: an array of CIDs, not a flattened CID list.
    session.request(
        json!({"jsonrpc":"2.0", "id":1, "method":"bitswap_unstable_stream", "params":[[cid]]})
            .to_string(),
    )?;
    let responses = futures_util::stream::unfold(&mut session.responses, |responses| async {
        responses.next().await.map(|response| (response, responses))
    });
    futures_util::pin_mut!(responses);
    receive_block(&mut responses, cid, progress).await
}

async fn receive_block(
    responses: &mut (impl Stream<Item = String> + Unpin),
    cid: &str,
    progress: &mut Progress,
) -> Result<Vec<u8>> {
    progress.phase("await_subscription");
    let response = upstream_response(responses).await?;
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
    progress.phase("await_block");
    loop {
        let notification = upstream_response(responses).await?;
        if accept_event(&notification, subscription, cid, &mut block)? {
            let block = block.context("Bitswap completed without a block")?;
            log::debug!(
                "host: content block_received {} block_bytes={}",
                progress,
                block.len(),
            );
            return Ok(block);
        }
        progress.phase("await_done");
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

    #[test]
    fn advertised_request_bounds_match_enforced_boundaries() {
        let advertised = capabilities(&[]);
        let mut request = json!({
            "version": advertised.protocol_version,
            "cid": "c",
            "max_bytes": advertised.limits.max_file_bytes,
        });
        assert!(parse_request(&request.to_string()).is_ok());
        request["max_bytes"] = json!(advertised.limits.max_file_bytes + 1);
        assert!(parse_request(&request.to_string()).is_err());
        request["max_bytes"] = json!(0);
        let mut text = request.to_string();
        text.extend(std::iter::repeat_n(
            ' ',
            advertised.limits.max_control_bytes as usize - text.len(),
        ));
        assert!(parse_request(&text).is_ok());
        text.push(' ');
        assert!(parse_request(&text).is_err());
        request["version"] = json!(advertised.protocol_version + 1);
        assert!(parse_request(&request.to_string()).is_err());
    }

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

#[cfg(test)]
mod diagnostic_tests {
    use super::*;
    use futures_util::{FutureExt, stream};

    fn reply() -> String {
        json!({"jsonrpc":"2.0", "id":1, "result":"s"}).to_string()
    }

    fn event(result: serde_json::Value) -> String {
        json!({
            "jsonrpc":"2.0", "method":"bitswap_unstable_streamEvent",
            "params":{"subscription":"s", "result":result},
        })
        .to_string()
    }

    #[tokio::test]
    async fn upstream_timeout_preserves_stage_and_original_error() {
        for received_block in [false, true] {
            let mut messages = vec![reply()];
            if received_block {
                messages.push(event(json!({
                    "event":"streamItem", "cid":"c", "value":"0x0102",
                })));
            }
            messages.push(event(json!({
                "event":"streamItemError", "cid":"c", "code":-32811,
                "message":"Request timeout.",
            })));
            let mut progress = Progress::new();
            progress.begin_block();
            let error = receive_block(&mut stream::iter(messages), "c", &mut progress)
                .await
                .unwrap_err();
            assert_eq!(
                progress.phase,
                if received_block {
                    "await_done"
                } else {
                    "await_block"
                }
            );
            // Receipt of bytes is not CID verification or delivery to the caller.
            assert_eq!(progress.verified_blocks, 0);
            assert_eq!(progress.verified_bytes, 0);
            assert_eq!(progress.sent_bytes, 0);
            let error = progress.error("test-chain", error);
            assert!(error.root_cause().to_string().contains("-32811"));
            let public_error = format!("{error:#}");
            assert!(public_error.contains("Request timeout."));
            assert!(public_error.contains(progress.phase));
        }
    }

    #[test]
    fn cancelled_wait_retains_the_last_stage_without_claiming_completion() {
        let item = event(json!({"event":"streamItem", "cid":"c", "value":"0x0102"}));
        for (messages, phase) in [
            (vec![], "await_subscription"),
            (vec![reply()], "await_block"),
            (vec![reply(), item], "await_done"),
        ] {
            let mut responses = stream::iter(messages).chain(stream::pending());
            let mut progress = Progress::new();
            progress.begin_block();
            assert!(
                receive_block(&mut responses, "c", &mut progress)
                    .now_or_never()
                    .is_none()
            );
            assert_eq!(progress.phase, phase);
            assert_eq!(progress.block, 1);
            assert_eq!(progress.verified_blocks, 0);
            assert_eq!(progress.sent_bytes, 0);
        }
    }

    #[tokio::test]
    async fn diagnosed_stream_still_requires_done_and_rejects_premature_eof() {
        let item = event(json!({"event":"streamItem", "cid":"c", "value":"0x0102"}));
        let mut progress = Progress::new();
        progress.begin_block();
        let mut incomplete = stream::iter(vec![reply(), item.clone()]);
        assert!(
            receive_block(&mut incomplete, "c", &mut progress)
                .await
                .is_err()
        );
        assert_eq!(progress.phase, "await_done");
        let mut complete = stream::iter(vec![reply(), item, event(json!({"event":"streamDone"}))]);
        assert_eq!(
            receive_block(&mut complete, "c", &mut progress)
                .await
                .unwrap(),
            [1, 2],
        );
    }
}
