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
    CHUNK_BYTES, Control, IO_TIMEOUT, MAX_BLOCKS, MAX_CONTROL_BYTES, MAX_FILE_BYTES, Receipt,
    Request, TRANSFER_TIMEOUT, Walker,
};

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
pub async fn fetch(
    socket_path: &Path,
    chain: &str,
    cid: &str,
    output: &Path,
    options: FetchOptions,
) -> Result<Receipt> {
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
    tokio::time::timeout(options.timeout, async {
        // Fail early, but rely on atomic no-clobber publication for the race.
        match std::fs::symlink_metadata(output) {
            Ok(_) => bail!("output already exists"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
            Err(error) => return Err(error).context("checking output path"),
        }
        let mut socket = crate::client::connect_socket(
            socket_path,
            format!("ws://localhost/content/{chain}"),
            CHUNK_BYTES,
        )
        .await?;
        let request = serde_json::to_string(&Request {
            version: 1,
            cid: cid.to_owned(),
            max_bytes: options.max_bytes,
        })?;
        tokio::time::timeout(IO_TIMEOUT, socket.send(Message::Text(request.into())))
            .await
            .context("content request write timed out")??;
        receive_to_file(socket, chain, cid, output, options.max_bytes).await
    })
    .await
    .context("content download timed out")?
}

async fn receive_to_file(
    mut socket: WebSocketStream<UnixStream>,
    chain: &str,
    cid: &str,
    output: &Path,
    max_bytes: u64,
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
                    text.len() <= MAX_CONTROL_BYTES,
                    "content control exceeds limit"
                );
                match serde_json::from_str::<Control>(&text).context("invalid content control")? {
                    Control::Started {
                        version,
                        cid: received,
                        chain: received_chain,
                    } => {
                        ensure!(
                            !started && version == 1 && received == cid && received_chain == chain,
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
                            count == bytes && blocks > 0 && blocks <= MAX_BLOCKS as u64,
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
                    !chunk.is_empty() && chunk.len() <= CHUNK_BYTES,
                    "content chunk exceeds limit or is empty"
                );
                bytes = bytes
                    .checked_add(chunk.len() as u64)
                    .context("content byte count overflow")?;
                ensure!(
                    bytes <= max_bytes && bytes <= MAX_FILE_BYTES,
                    "content exceeds requested limit"
                );
                digest.update(&chunk);
                file.write_all(&chunk)
                    .await
                    .context("writing partial file")?;
            }
            Message::Ping(_) => {
                tokio::time::timeout(IO_TIMEOUT, socket.flush())
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
    use tokio_tungstenite::tungstenite::protocol::Role;

    fn text(value: serde_json::Value) -> Message {
        Message::Text(value.to_string().into())
    }
    fn started() -> Message {
        text(json!({"event":"started","version":1,"cid":"test-cid","chain":"bulletin"}))
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
        let result = receive_to_file(client, "bulletin", "test-cid", output, limit).await;
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
            let future = receive_to_file(client, "bulletin", "test-cid", &output, MAX_FILE_BYTES);
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
}
