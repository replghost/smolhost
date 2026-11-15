// SPDX-License-Identifier: GPL-3.0-or-later

//! One-shot, cancellation-safe downloads to a private temporary file. Only a
//! validated completion marker permits atomic no-clobber publication. Dropping
//! the fetch future closes its connection and removes its partial file.

use std::{path::Path, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use sha2::{Digest, Sha256};
use tokio::{io::AsyncWriteExt, net::UnixStream};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

use super::{
    CHUNK_BYTES, Control, IO_TIMEOUT, MAX_BLOCK_BYTES, MAX_BLOCKS, MAX_CONTROL_BYTES,
    MAX_DAG_OVERHEAD_BYTES, MAX_DEPTH, MAX_FILE_BYTES, PROTOCOL_VERSION, Receipt, Request,
    TRANSFER_TIMEOUT, Walker,
};
use crate::discovery::HostInfo;

struct Policy<'a> {
    path: &'a str,
    max_bytes: u64,
    max_blocks: u64,
    chunk_bytes: usize,
    control_bytes: usize,
    handshake: Duration,
    request: Duration,
    write: Duration,
    transfer: Duration,
}

fn negotiate<'a>(host: &'a HostInfo, chain: &str, cid: &str, allowance: u64) -> Result<Policy<'a>> {
    ensure!(
        host.chains.iter().filter(|entry| entry.id == chain).count() == 1,
        "content chain must be configured exactly once"
    );
    let content = host
        .content
        .as_ref()
        .context("host does not advertise content retrieval")?;
    ensure!(
        content.protocol_version == PROTOCOL_VERSION,
        "unsupported content protocol version"
    );
    let mut endpoints = content
        .endpoints
        .iter()
        .filter(|endpoint| endpoint.chain_id == chain);
    let endpoint = endpoints
        .next()
        .context("no advertised content endpoint for chain")?;
    ensure!(
        endpoints.next().is_none(),
        "ambiguous content endpoint for chain"
    );
    let path = endpoint.path.as_str();
    ensure!(
        path.len() > 1
            && path.len() <= 256
            && path.starts_with('/')
            && path[1..].split('/').all(|part| {
                !part.is_empty()
                    && part
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte))
            }),
        "invalid advertised content path"
    );
    ensure!(
        content
            .endpoints
            .iter()
            .filter(|other| other.path == path)
            .count()
            == 1,
        "ambiguous advertised content path"
    );
    // Walker has already validated canonical CIDv1 and the two single-byte codecs.
    // Use smoldot's parser and multihash decoder rather than duplicating CID/crypto logic.
    let parsed: smoldot::libp2p::cid::Cid = cid.parse().context("unsupported CID")?;
    let codec = u64::from(parsed.as_ref()[1]);
    let hash = parsed.prefix().multihash_type() as u64;
    ensure!(
        content.cid_versions.contains(&1),
        "host does not support requested CID version"
    );
    ensure!(
        content.multibases.iter().any(|base| base == "base32lower"),
        "host does not support requested CID multibase"
    );
    ensure!(
        content.codecs.contains(&codec),
        "host does not support requested CID codec"
    );
    ensure!(
        content.hashes.contains(&hash),
        "host does not support requested CID hash"
    );
    if codec == 0x70 {
        ensure!(
            ["file", "raw"].iter().all(|kind| content
                .unixfs_types
                .iter()
                .any(|value| value.as_str() == *kind)),
            "host does not support required UnixFS file/raw forms"
        );
    }
    let limits = &content.limits;
    ensure!(
        limits.max_block_bytes > 0
            && limits.max_block_bytes <= MAX_BLOCK_BYTES as u64
            && limits.max_depth > 0
            && limits.max_depth <= MAX_DEPTH as u64
            && limits.max_dag_overhead_bytes <= MAX_DAG_OVERHEAD_BYTES,
        "incompatible content DAG limits"
    );
    ensure!(
        limits.max_blocks > 0 && limits.max_concurrent_transfers > 0 && limits.max_connections > 0,
        "invalid content count limits"
    );
    ensure!(
        limits.max_chunk_bytes > 0
            && limits.max_chunk_bytes <= CHUNK_BYTES as u64
            && limits.max_control_bytes > 0
            && limits.max_control_bytes <= MAX_CONTROL_BYTES as u64,
        "incompatible content framing limits"
    );
    let deadlines = &content.deadlines;
    ensure!(
        deadlines.handshake_ms > 0
            && deadlines.request_ms > 0
            && deadlines.write_ms > 0
            && deadlines.transfer_ms > 0,
        "invalid content deadlines"
    );
    Ok(Policy {
        path,
        max_bytes: allowance.min(limits.max_file_bytes),
        max_blocks: (MAX_BLOCKS as u64).min(limits.max_blocks),
        chunk_bytes: limits.max_chunk_bytes as usize,
        control_bytes: limits.max_control_bytes as usize,
        handshake: IO_TIMEOUT.min(Duration::from_millis(deadlines.handshake_ms)),
        request: IO_TIMEOUT.min(Duration::from_millis(deadlines.request_ms)),
        write: IO_TIMEOUT.min(Duration::from_millis(deadlines.write_ms)),
        transfer: TRANSFER_TIMEOUT.min(Duration::from_millis(deadlines.transfer_ms)),
    })
}

/// Caller limits can tighten, but cannot relax, the daemon's hard bounds.
#[derive(Debug, Clone, Copy)]
pub struct FetchOptions {
    pub max_bytes: u64,
    pub timeout: Duration,
}

impl Default for FetchOptions {
    fn default() -> Self {
        Self {
            max_bytes: MAX_FILE_BYTES,
            timeout: TRANSFER_TIMEOUT,
        }
    }
}

/// Retrieve one CID without retries, then sync and publish without replacing an
/// existing path. The receipt verifies transport integrity, not chain inclusion
/// or authorship. Cancellation (including dropping this future) never publishes
/// an incomplete file. The daemon never receives the destination pathname.
/// Discovery and transfer share the caller's total deadline. Missing or incompatible
/// capabilities fail closed; only the advertised path on the same socket is used.
pub async fn fetch(
    socket_path: &Path,
    chain: &str,
    cid: &str,
    output: &Path,
    options: FetchOptions,
) -> Result<Receipt> {
    let began = tokio::time::Instant::now();
    crate::client::validate_chain_id(chain)?;
    ensure!(
        options.max_bytes <= MAX_FILE_BYTES,
        "max_bytes exceeds content limit"
    );
    ensure!(
        !options.timeout.is_zero() && options.timeout <= TRANSFER_TIMEOUT,
        "timeout must be greater than zero and at most 180 seconds"
    );
    Walker::new(cid, options.max_bytes)?;
    tokio::time::timeout_at(began + options.timeout, async {
        let host = crate::client::discover(socket_path).await?;
        let policy = negotiate(&host, chain, cid, options.max_bytes)?;
        let deadline = began + options.timeout.min(policy.transfer);
        let request = serde_json::to_string(&Request {
            version: PROTOCOL_VERSION,
            cid: cid.to_owned(),
            max_bytes: policy.max_bytes,
        })?;
        ensure!(
            request.len() <= policy.control_bytes,
            "content request exceeds advertised control limit"
        );
        // Canonical CIDs and validated chain IDs need no JSON escaping. Include
        // the largest completion counts permitted by this request, not just the
        // request envelope: a usable v1 route must also fit its success markers.
        let started_bytes = br#"{"event":"started","version":,"cid":"","chain":""}"#.len()
            + PROTOCOL_VERSION.ilog10() as usize
            + 1
            + cid.len()
            + chain.len();
        let completed_bytes = br#"{"event":"completed","cid":"","bytes":,"blocks":,"sha256":""}"#
            .len()
            + cid.len()
            + 64
            + policy.max_bytes.max(1).ilog10() as usize
            + 1
            + policy.max_blocks.ilog10() as usize
            + 1;
        ensure!(
            started_bytes.max(completed_bytes) <= policy.control_bytes,
            "content success markers exceed advertised control limit"
        );
        ensure!(
            tokio::time::Instant::now() < deadline,
            "content download timed out"
        );
        // Fail early, but rely on atomic no-clobber publication for the race.
        match std::fs::symlink_metadata(output) {
            Ok(_) => bail!("output already exists"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
            Err(error) => return Err(error).context("checking output path"),
        }
        tokio::time::timeout_at(deadline, async {
            let mut socket = tokio::time::timeout(
                policy.handshake,
                crate::client::connect_socket(
                    socket_path,
                    format!("ws://localhost{}", policy.path),
                    policy.chunk_bytes.max(policy.control_bytes),
                ),
            )
            .await
            .context("content handshake timed out")??;
            tokio::time::timeout(
                policy.request.min(policy.write),
                socket.send(Message::Text(request.into())),
            )
            .await
            .context("content request write timed out")??;
            receive_to_file(socket, chain, cid, output, &policy, deadline).await
        })
        .await
        .context("content download timed out")?
    })
    .await
    .context("content download timed out")?
}

async fn receive_to_file(
    mut socket: WebSocketStream<UnixStream>,
    chain: &str,
    cid: &str,
    output: &Path,
    policy: &Policy<'_>,
    deadline: tokio::time::Instant,
) -> Result<Receipt> {
    ensure!(output.file_name().is_some(), "output must name a file");
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    // tempfile creates the file with 0600 permissions and unlinks it on Drop.
    // Keep the ownership guard in this future, not in a detached blocking task.
    let temporary = tempfile::Builder::new()
        .prefix(".smolhost-content-")
        .tempfile_in(parent)
        .context("creating private partial file")?;
    let mut file = tokio::fs::File::from_std(temporary.reopen()?);
    let mut digest = Sha256::new();
    let mut bytes = 0u64;
    let mut started = false;
    let receipt = loop {
        let message = socket
            .next()
            .await
            .context("content stream closed without completion")??;
        match message {
            Message::Text(text) => {
                ensure!(
                    text.len() <= policy.control_bytes,
                    "content control exceeds limit"
                );
                match serde_json::from_str::<Control>(&text).context("invalid content control")? {
                    Control::Started {
                        version,
                        cid: received,
                        chain: received_chain,
                    } => {
                        ensure!(
                            !started
                                && version == PROTOCOL_VERSION
                                && received == cid
                                && received_chain == chain,
                            "unexpected content started marker"
                        );
                        started = true;
                    }
                    Control::Completed {
                        cid: received,
                        bytes: count,
                        blocks,
                        sha256,
                    } => {
                        ensure!(
                            started && received == cid,
                            "unexpected content completion marker"
                        );
                        ensure!(
                            count == bytes && blocks > 0 && blocks <= policy.max_blocks,
                            "content completion count mismatch"
                        );
                        let actual = format!("{:x}", digest.finalize());
                        ensure!(sha256 == actual, "content completion SHA256 mismatch");
                        break Receipt {
                            cid: received,
                            bytes: count,
                            blocks,
                            sha256,
                        };
                    }
                    Control::Error { message } => bail!(
                        "content retrieval failed: {}",
                        message.chars().take(512).collect::<String>()
                    ),
                }
            }
            Message::Binary(chunk) => {
                ensure!(started, "content bytes arrived before started marker");
                ensure!(
                    !chunk.is_empty() && chunk.len() <= policy.chunk_bytes,
                    "content chunk exceeds limit or is empty"
                );
                bytes = bytes
                    .checked_add(chunk.len() as u64)
                    .context("content byte count overflow")?;
                ensure!(
                    bytes <= policy.max_bytes && bytes <= MAX_FILE_BYTES,
                    "content exceeds requested limit"
                );
                digest.update(&chunk);
                file.write_all(&chunk)
                    .await
                    .context("writing partial file")?;
            }
            Message::Ping(_) => {
                tokio::time::timeout(policy.write, socket.flush())
                    .await
                    .context("content pong timed out")??;
            }
            Message::Pong(_) => (),
            Message::Close(_) => bail!("content stream closed without completion"),
            _ => bail!("unexpected content frame"),
        }
    };
    file.flush().await.context("flushing verified content")?;
    file.sync_all().await.context("syncing verified content")?;
    drop(file);
    ensure!(
        tokio::time::Instant::now() < deadline,
        "content download timed out"
    );
    // No await between publication and return: cancellation cannot strand a
    // published file while reporting an incomplete download to the caller.
    temporary
        .persist_noclobber(output)
        .map_err(|error| error.error)
        .context("publishing content without overwriting output")?;
    Ok(receipt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::net::UnixListener;
    use tokio_tungstenite::tungstenite::protocol::Role;
    use tokio_tungstenite::{
        accept_hdr_async,
        tungstenite::handshake::server::{Request as HandshakeRequest, Response},
    };

    fn text(value: serde_json::Value) -> Message {
        Message::Text(value.to_string().into())
    }
    fn started() -> Message {
        text(
            json!({"event":"started","version":PROTOCOL_VERSION,"cid":"test-cid","chain":"bulletin"}),
        )
    }
    fn completed(data: &[u8]) -> Message {
        text(
            json!({"event":"completed","cid":"test-cid","bytes":data.len(),"blocks":1,"sha256":format!("{:x}", Sha256::digest(data))}),
        )
    }
    async fn sockets() -> (WebSocketStream<UnixStream>, WebSocketStream<UnixStream>) {
        let (a, b) = UnixStream::pair().unwrap();
        (
            WebSocketStream::from_raw_socket(a, Role::Client, None).await,
            WebSocketStream::from_raw_socket(b, Role::Server, None).await,
        )
    }
    async fn download(messages: Vec<Message>, output: &Path, limit: u64) -> Result<Receipt> {
        let (client, mut server) = sockets().await;
        let producer = tokio::spawn(async move {
            for message in messages {
                if server.send(message).await.is_err() {
                    return;
                }
            }
            let _ = server.close(None).await;
        });
        let host = serde_json::from_value(catalog()).unwrap();
        let policy = negotiate(&host, "bulletin", &fixture_cid(0x55), limit).unwrap();
        let result = receive_to_file(
            client,
            "bulletin",
            "test-cid",
            output,
            &policy,
            tokio::time::Instant::now() + TRANSFER_TIMEOUT,
        )
        .await;
        producer.await.unwrap();
        result
    }

    #[tokio::test]
    async fn content_save_verifies_digest_counts_and_completion_before_publication() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("file");
        let data = b"verified bytes";
        let cases = vec![
            vec![started(), Message::Binary(data.to_vec().into())],
            vec![
                started(),
                Message::Binary(data.to_vec().into()),
                completed(b"wrong digest!!"),
            ],
            vec![
                started(),
                Message::Binary(data.to_vec().into()),
                completed(b"wrong length"),
            ],
            vec![Message::Binary(data.to_vec().into()), completed(data)],
            vec![completed(b"")],
            vec![started(), started(), completed(b"")],
            vec![
                started(),
                text(
                    json!({"event":"completed","cid":"test-cid","bytes":0,"blocks":0,"sha256":format!("{:x}",Sha256::digest(b""))}),
                ),
            ],
            vec![
                started(),
                text(json!({"event":"error","message":"missing block"})),
            ],
            vec![started(), Message::Binary(vec![0; CHUNK_BYTES + 1].into())],
        ];
        for messages in cases {
            assert!(download(messages, &output, MAX_FILE_BYTES).await.is_err());
            assert!(!output.exists());
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
        }
        assert!(
            download(
                vec![
                    started(),
                    Message::Binary(data.to_vec().into()),
                    completed(data)
                ],
                &output,
                1
            )
            .await
            .is_err()
        );
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
        let receipt = download(
            vec![
                started(),
                Message::Binary(data.to_vec().into()),
                completed(data),
            ],
            &output,
            MAX_FILE_BYTES,
        )
        .await
        .unwrap();
        assert_eq!(receipt.bytes, data.len() as u64);
        assert_eq!(std::fs::read(&output).unwrap(), data);
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&output).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn content_save_never_clobbers_existing_file_or_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("file");
        std::fs::write(&output, b"original").unwrap();
        assert!(
            download(vec![started(), completed(b"")], &output, MAX_FILE_BYTES)
                .await
                .is_err()
        );
        assert_eq!(std::fs::read(&output).unwrap(), b"original");
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&output, &link).unwrap();
        assert!(
            download(vec![started(), completed(b"")], &link, MAX_FILE_BYTES)
                .await
                .is_err()
        );
        assert_eq!(std::fs::read(&output).unwrap(), b"original");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 2);
    }

    #[tokio::test]
    async fn content_save_cancel_and_timeout_remove_partial_and_disconnect() {
        for timeout in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let output = dir.path().join("file");
            let (client, mut server) = sockets().await;
            server.send(started()).await.unwrap();
            server
                .send(Message::Binary(b"partial".to_vec().into()))
                .await
                .unwrap();
            let host = serde_json::from_value(catalog()).unwrap();
            let policy = negotiate(&host, "bulletin", &fixture_cid(0x55), MAX_FILE_BYTES).unwrap();
            let future = receive_to_file(
                client,
                "bulletin",
                "test-cid",
                &output,
                &policy,
                tokio::time::Instant::now() + TRANSFER_TIMEOUT,
            );
            if timeout {
                assert!(
                    tokio::time::timeout(Duration::from_millis(30), future)
                        .await
                        .is_err()
                );
            } else {
                let mut future = Box::pin(future);
                tokio::select! {
                    result = &mut future => panic!("download completed without marker: {result:?}"),
                    _ = tokio::time::sleep(Duration::from_millis(30)) => (),
                }
                assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
                drop(future);
            }
            assert!(!output.exists());
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
            let disconnected = tokio::time::timeout(IO_TIMEOUT, server.next())
                .await
                .unwrap();
            assert!(matches!(
                disconnected,
                None | Some(Err(_)) | Some(Ok(Message::Close(_)))
            ));
        }
    }

    fn fixture_cid(codec: u8) -> String {
        smoldot::libp2p::cid::CidPrefix::from_bytes(vec![1, codec, 0x12, 32])
            .unwrap()
            .with_digest_of(b"verified bytes")
            .to_string()
    }

    // These are deterministic protocol peers, not evidence of live Bitswap retrieval.
    fn catalog() -> serde_json::Value {
        json!({
            "protocol_version": crate::discovery::PROTOCOL_VERSION,
            "read_only": true,
            "chains": [{
                "id": "bulletin", "name": "Bulletin", "genesis_hash": "0x11",
                "relay_chain": null, "para_id": null, "properties": {}, "rpc_methods": []
            }],
            "content": {
                "protocol_version": PROTOCOL_VERSION,
                "endpoints": [{"chain_id": "bulletin", "path": "/alternate/files_v1"}],
                "cid_versions": [1], "multibases": ["base32lower"],
                "codecs": [0x55, 0x70], "hashes": [0x12, 0x1b, 0xb220],
                "unixfs_types": ["file", "raw"],
                "limits": {
                    "max_file_bytes": MAX_FILE_BYTES, "max_block_bytes": MAX_BLOCK_BYTES,
                    "max_blocks": MAX_BLOCKS, "max_depth": MAX_DEPTH,
                    "max_dag_overhead_bytes": MAX_DAG_OVERHEAD_BYTES,
                    "max_chunk_bytes": CHUNK_BYTES, "max_control_bytes": MAX_CONTROL_BYTES,
                    "max_concurrent_transfers": 4, "max_connections": 16
                },
                "deadlines": {
                    "handshake_ms": 5000, "request_ms": 5000,
                    "write_ms": 5000, "transfer_ms": 180000
                }
            }
        })
    }

    async fn accept(listener: &UnixListener, path: &'static str) -> WebSocketStream<UnixStream> {
        let (stream, _) = listener.accept().await.unwrap();
        accept_hdr_async(
            stream,
            move |request: &HandshakeRequest, response: Response| {
                assert_eq!(request.uri().path(), path);
                Ok(response)
            },
        )
        .await
        .unwrap()
    }

    async fn advertise(listener: &UnixListener, host: serde_json::Value) {
        let mut peer = accept(listener, "/host").await;
        let request: serde_json::Value =
            serde_json::from_str(&peer.next().await.unwrap().unwrap().into_text().unwrap())
                .unwrap();
        assert_eq!(request["method"], "host_discover");
        peer.send(text(
            json!({"jsonrpc":"2.0","id":request["id"],"result":host}),
        ))
        .await
        .unwrap();
        let _ = peer.next().await;
    }

    async fn rejected(host: serde_json::Value, cid: &str, expected: &str) {
        rejected_after(host, cid, expected, Duration::ZERO).await;
    }

    async fn rejected_after(host: serde_json::Value, cid: &str, expected: &str, delay: Duration) {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("rpc.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let destination = tempfile::tempdir().unwrap();
        let output = destination.path().join("file");
        let (done, finished) = tokio::sync::oneshot::channel();
        tokio::join!(
            async {
                let error = fetch(
                    &socket,
                    "bulletin",
                    cid,
                    &output,
                    FetchOptions {
                        timeout: Duration::from_secs(2),
                        ..FetchOptions::default()
                    },
                )
                .await
                .unwrap_err();
                assert!(
                    format!("{error:#}").contains(expected),
                    "{error:#}; expected {expected}"
                );
                done.send(()).unwrap();
            },
            async {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                advertise(&listener, host).await;
                tokio::select! {
                    biased;
                    _ = listener.accept() => panic!("rejected catalog opened content transport"),
                    _ = finished => (),
                }
            },
        );
        assert_eq!(std::fs::read_dir(destination.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn content_discovery_rejects_unavailable_ambiguous_and_unsupported_before_connect() {
        let cid = fixture_cid(0x55);
        let mut absent = catalog();
        absent.as_object_mut().unwrap().remove("content");
        rejected(absent, &cid, "does not advertise content").await;
        for (pointer, value, expected) in [
            (
                "/content/protocol_version",
                json!(PROTOCOL_VERSION + 1),
                "unsupported content protocol",
            ),
            ("/content/cid_versions", json!([0]), "requested CID version"),
            (
                "/content/multibases",
                json!(["base58btc"]),
                "requested CID multibase",
            ),
            ("/content/codecs", json!([0x70]), "requested CID codec"),
            ("/content/hashes", json!([0x1b]), "requested CID hash"),
            (
                "/content/endpoints",
                json!([]),
                "no advertised content endpoint",
            ),
            ("/chains", json!([]), "configured exactly once"),
        ] {
            let mut host = catalog();
            *host.pointer_mut(pointer).unwrap() = value;
            rejected(host, &cid, expected).await;
        }
        for field in ["chains", "endpoints"] {
            let mut host = catalog();
            let list = if field == "chains" {
                &mut host["chains"]
            } else {
                &mut host["content"]["endpoints"]
            };
            let duplicate = list[0].clone();
            list.as_array_mut().unwrap().push(duplicate);
            rejected(
                host,
                &cid,
                if field == "chains" {
                    "configured exactly once"
                } else {
                    "ambiguous content endpoint"
                },
            )
            .await;
        }
        let mut host = catalog();
        host["content"]["endpoints"]
            .as_array_mut()
            .unwrap()
            .push(json!({"chain_id":"other","path":"/alternate/files_v1"}));
        rejected(host, &cid, "ambiguous advertised content path").await;
        let mut host = catalog();
        host["content"]["unixfs_types"] = json!(["file"]);
        rejected(host, &fixture_cid(0x70), "UnixFS file/raw forms").await;
    }

    #[tokio::test]
    async fn content_discovery_rejects_malformed_paths_and_limits_before_connect() {
        let cid = fixture_cid(0x55);
        for path in [
            "",
            "/",
            "//evil/route",
            "https://evil/route",
            "/x?query",
            "/x#fragment",
            "/x%2fy",
            "/x/../y",
            "/x/./y",
            "/x//y",
            "/x/",
            "/x\\y",
            "/x\n",
            "/é",
            &format!("/{}", "x".repeat(256)),
        ] {
            let mut host = catalog();
            host["content"]["endpoints"][0]["path"] = json!(path);
            rejected(host, &cid, "invalid advertised content path").await;
        }
        for (field, value, expected) in [
            ("max_block_bytes", 0, "DAG limits"),
            ("max_block_bytes", MAX_BLOCK_BYTES as u64 + 1, "DAG limits"),
            ("max_depth", 0, "DAG limits"),
            ("max_depth", MAX_DEPTH as u64 + 1, "DAG limits"),
            (
                "max_dag_overhead_bytes",
                MAX_DAG_OVERHEAD_BYTES + 1,
                "DAG limits",
            ),
            ("max_blocks", 0, "count limits"),
            ("max_connections", 0, "count limits"),
            ("max_concurrent_transfers", 0, "count limits"),
            ("max_chunk_bytes", 0, "framing limits"),
            ("max_chunk_bytes", CHUNK_BYTES as u64 + 1, "framing limits"),
            ("max_control_bytes", 0, "framing limits"),
            (
                "max_control_bytes",
                MAX_CONTROL_BYTES as u64 + 1,
                "framing limits",
            ),
            (
                "max_control_bytes",
                1,
                "request exceeds advertised control limit",
            ),
            (
                "max_control_bytes",
                120,
                "success markers exceed advertised control limit",
            ),
        ] {
            let mut host = catalog();
            host["content"]["limits"][field] = json!(value);
            rejected(host, &cid, expected).await;
        }
        for field in ["handshake_ms", "request_ms", "write_ms", "transfer_ms"] {
            let mut host = catalog();
            host["content"]["deadlines"][field] = json!(0);
            rejected(host, &cid, "invalid content deadlines").await;
        }
        let mut host = catalog();
        host["content"]["limits"]["max_file_bytes"] = json!("unbounded");
        rejected(host, &cid, "invalid host discovery response").await;
    }

    #[tokio::test]
    async fn content_discovery_uses_alternate_path_and_tightens_request_and_receipt_limits() {
        for case in [
            "success", "caller", "bytes", "chunk", "control", "blocks", "remote",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let socket = dir.path().join("rpc.sock");
            let listener = UnixListener::bind(&socket).unwrap();
            let destination = tempfile::tempdir().unwrap();
            let output = destination.path().join("file");
            let cid = fixture_cid(0x55);
            let mut host = catalog();
            host["content"]["limits"]["max_file_bytes"] = json!(14);
            host["content"]["limits"]["max_blocks"] = json!(1);
            host["content"]["limits"]["max_chunk_bytes"] = json!(8);
            host["content"]["limits"]["max_control_bytes"] = json!(512);
            host["content"]["limits"]["max_block_bytes"] = json!(128);
            host["content"]["limits"]["max_depth"] = json!(1);
            host["content"]["limits"]["max_dag_overhead_bytes"] = json!(0);
            // Unknown extras must not invalidate a format this client does understand.
            host["content"]["codecs"]
                .as_array_mut()
                .unwrap()
                .push(json!(0xffff));
            host["content"]["hashes"]
                .as_array_mut()
                .unwrap()
                .push(json!(0xffff));
            host["content"]["cid_versions"]
                .as_array_mut()
                .unwrap()
                .push(json!(99));
            host["content"]["multibases"]
                .as_array_mut()
                .unwrap()
                .push(json!("future"));
            let allowance = if case == "caller" { 7 } else { MAX_FILE_BYTES };
            let (result, ()) = tokio::join!(
                fetch(
                    &socket,
                    "bulletin",
                    &cid,
                    &output,
                    FetchOptions {
                        max_bytes: allowance,
                        timeout: Duration::from_secs(2),
                    }
                ),
                async {
                    advertise(&listener, host).await;
                    let mut peer = accept(&listener, "/alternate/files_v1").await;
                    let request: serde_json::Value = serde_json::from_str(
                        &peer.next().await.unwrap().unwrap().into_text().unwrap(),
                    )
                    .unwrap();
                    assert_eq!(request["max_bytes"], allowance.min(14));
                    assert_eq!(request["version"], PROTOCOL_VERSION);
                    assert_eq!(request["cid"], cid);
                    peer.send(text(json!({"event":"started","version":PROTOCOL_VERSION,"cid":cid,"chain":"bulletin"}))).await.unwrap();
                    let data = if case == "caller" {
                        &b"verified bytes"[..7]
                    } else {
                        &b"verified bytes"[..]
                    };
                    let mut messages = Vec::new();
                    match case {
                        "chunk" => messages.push(Message::Binary(vec![0; 9].into())),
                        "control" => {
                            messages.push(text(json!({"event":"error","message":"x".repeat(512)})))
                        }
                        "remote" => messages.push(text(
                            json!({"event":"error","message":"missing native block"}),
                        )),
                        _ => {
                            for chunk in data.chunks(7) {
                                messages.push(Message::Binary(chunk.to_vec().into()));
                            }
                            if case == "bytes" {
                                messages.push(Message::Binary(vec![0].into()));
                            }
                            messages.push(text(json!({
                                "event":"completed", "cid":cid, "bytes":data.len(),
                                "blocks":if case == "blocks" { 2 } else { 1 },
                                "sha256":format!("{:x}", Sha256::digest(data)),
                            })));
                        }
                    }
                    for message in messages {
                        if peer.send(message).await.is_err() {
                            break;
                        }
                    }
                    let _ = peer.next().await;
                },
            );
            if case == "success" || case == "caller" {
                assert_eq!(result.unwrap().bytes, allowance.min(14));
                assert_eq!(
                    std::fs::read(&output).unwrap(),
                    &b"verified bytes"[..allowance.min(14) as usize]
                );
                assert_eq!(std::fs::read_dir(destination.path()).unwrap().count(), 1);
            } else {
                let expected = match case {
                    "bytes" => "requested limit",
                    "chunk" => "chunk exceeds limit",
                    "control" => "control exceeds limit",
                    "blocks" => "completion count mismatch",
                    "remote" => "content retrieval failed: missing native block",
                    _ => unreachable!(),
                };
                let error = result.unwrap_err();
                // Oversized WS messages can be rejected at the framing layer first.
                assert!(
                    format!("{error:#}").contains(expected)
                        || (case == "control"
                            && format!("{error:#}").contains("Space limit exceeded")),
                    "{case}: {error:#}"
                );
                assert_eq!(std::fs::read_dir(destination.path()).unwrap().count(), 0);
            }
        }
    }

    #[tokio::test]
    async fn content_discovery_total_and_advertised_transfer_deadlines_remove_partial() {
        for discovery_stalls in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let socket = dir.path().join("rpc.sock");
            let listener = UnixListener::bind(&socket).unwrap();
            let destination = tempfile::tempdir().unwrap();
            let output = destination.path().join("file");
            let cid = fixture_cid(0x55);
            let mut host = catalog();
            host["content"]["deadlines"]["transfer_ms"] = json!(500);
            let (result, ()) = tokio::join!(
                fetch(
                    &socket,
                    "bulletin",
                    &cid,
                    &output,
                    FetchOptions {
                        timeout: if discovery_stalls {
                            Duration::from_millis(500)
                        } else {
                            Duration::from_secs(3)
                        },
                        ..FetchOptions::default()
                    }
                ),
                async {
                    if discovery_stalls {
                        let mut peer = accept(&listener, "/host").await;
                        let _ = peer.next().await.unwrap().unwrap();
                        let _ = peer.next().await;
                    } else {
                        advertise(&listener, host).await;
                        let mut peer = accept(&listener, "/alternate/files_v1").await;
                        let _ = peer.next().await.unwrap().unwrap();
                        peer.send(text(json!({"event":"started","version":PROTOCOL_VERSION,"cid":cid,"chain":"bulletin"}))).await.unwrap();
                        peer.send(Message::Binary(b"partial".to_vec().into()))
                            .await
                            .unwrap();
                        // No completion: the advertised transfer deadline must disconnect us,
                        // well before the caller's larger allowance.
                        tokio::time::timeout(Duration::from_secs(2), peer.next())
                            .await
                            .unwrap();
                    }
                },
            );
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("content download timed out")
            );
            assert_eq!(std::fs::read_dir(destination.path()).unwrap().count(), 0);
        }
    }

    #[tokio::test]
    async fn content_discovery_time_counts_against_negotiated_transfer_deadline() {
        let mut host = catalog();
        host["content"]["deadlines"]["transfer_ms"] = json!(1);
        rejected_after(
            host,
            &fixture_cid(0x55),
            "content download timed out",
            Duration::from_millis(30),
        )
        .await;
    }

    #[tokio::test]
    async fn content_discovery_tightens_handshake_deadline_without_creating_output() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("rpc.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let destination = tempfile::tempdir().unwrap();
        let output = destination.path().join("file");
        let cid = fixture_cid(0x55);
        let mut host = catalog();
        host["content"]["deadlines"]["handshake_ms"] = json!(100);
        let (result, ()) = tokio::join!(
            fetch(
                &socket,
                "bulletin",
                &cid,
                &output,
                FetchOptions {
                    timeout: Duration::from_secs(3),
                    ..FetchOptions::default()
                }
            ),
            async {
                use tokio::io::AsyncReadExt;
                advertise(&listener, host).await;
                let (mut stream, _) = listener.accept().await.unwrap();
                // Read the client's handshake but never reply. The stricter advertised
                // deadline, not the common five-second transport timeout, must close it.
                tokio::time::timeout(Duration::from_secs(2), async {
                    let mut bytes = [0; 1024];
                    while stream.read(&mut bytes).await.unwrap() != 0 {}
                })
                .await
                .unwrap();
            },
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("content handshake timed out")
        );
        assert_eq!(std::fs::read_dir(destination.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn content_discovery_cancellation_removes_partial_and_disconnects() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("rpc.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let destination = tempfile::tempdir().unwrap();
        let output = destination.path().join("file");
        let cid = fixture_cid(0x55);
        let (ready, received) = tokio::sync::oneshot::channel();
        tokio::join!(
            async {
                let mut download = Box::pin(fetch(
                    &socket,
                    "bulletin",
                    &cid,
                    &output,
                    FetchOptions::default(),
                ));
                tokio::select! {
                    result = &mut download => panic!("unexpected completion: {result:?}"),
                    result = received => result.unwrap(),
                }
                assert_eq!(std::fs::read_dir(destination.path()).unwrap().count(), 1);
                drop(download);
                assert_eq!(std::fs::read_dir(destination.path()).unwrap().count(), 0);
            },
            async {
                advertise(&listener, catalog()).await;
                let mut peer = accept(&listener, "/alternate/files_v1").await;
                let _ = peer.next().await.unwrap().unwrap();
                peer.send(text(json!({"event":"started","version":PROTOCOL_VERSION,"cid":cid,"chain":"bulletin"}))).await.unwrap();
                peer.send(Message::Binary(b"partial".to_vec().into()))
                    .await
                    .unwrap();
                peer.send(Message::Ping(vec![1].into())).await.unwrap();
                // Pong is a barrier: the preceding partial bytes were processed.
                assert!(matches!(peer.next().await, Some(Ok(Message::Pong(_)))));
                ready.send(()).unwrap();
                assert!(matches!(
                    peer.next().await,
                    None | Some(Err(_)) | Some(Ok(Message::Close(_)))
                ));
            },
        );
    }

    #[tokio::test]
    async fn content_discovery_publication_race_never_overwrites_destination() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("rpc.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let destination = tempfile::tempdir().unwrap();
        let output = destination.path().join("file");
        let cid = fixture_cid(0x55);
        let (result, ()) = tokio::join!(
            fetch(&socket, "bulletin", &cid, &output, FetchOptions::default()),
            async {
                advertise(&listener, catalog()).await;
                let mut peer = accept(&listener, "/alternate/files_v1").await;
                let _ = peer.next().await.unwrap().unwrap();
                // fetch has already performed its early output-existence check.
                std::fs::write(&output, b"original").unwrap();
                peer.send(text(json!({"event":"started","version":PROTOCOL_VERSION,"cid":cid,"chain":"bulletin"}))).await.unwrap();
                peer.send(Message::Binary(b"verified bytes".to_vec().into()))
                    .await
                    .unwrap();
                peer.send(text(json!({
                    "event":"completed", "cid":cid, "bytes":14, "blocks":1,
                    "sha256":format!("{:x}", Sha256::digest(b"verified bytes")),
                })))
                .await
                .unwrap();
                let _ = peer.next().await;
            },
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("publishing content without overwriting output")
        );
        assert_eq!(std::fs::read(&output).unwrap(), b"original");
        assert_eq!(std::fs::read_dir(destination.path()).unwrap().count(), 1);
    }
}
