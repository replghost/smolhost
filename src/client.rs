// SPDX-License-Identifier: GPL-3.0-or-later

//! Native, same-user Unix WebSocket clients. No reconnects, retries, or background
//! tasks are created. Discovery describes configuration, not chain readiness.

use std::{collections::VecDeque, path::Path, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::UnixStream;
use tokio_tungstenite::{
    WebSocketStream, client_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

use crate::discovery::{ChainInfo, DISCOVERY_METHOD, DISCOVERY_PATH, HostInfo, PROTOCOL_VERSION};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_SEND_BYTES: usize = 64 * 1024;
const MAX_RECEIVE_BYTES: usize = 8 * 1024 * 1024;
const MAX_NOTIFICATIONS: usize = 256;
type Socket = WebSocketStream<UnixStream>;

pub(crate) fn validate_chain_id(id: &str) -> Result<()> {
    ensure!(
        !id.is_empty()
            && id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte)),
        "chain must be a nonempty URL path component using letters, digits, '-' or '_'"
    );
    Ok(())
}

/// Shared authenticated transport policy for JSON-RPC and content clients.
pub(crate) async fn connect_socket(path: &Path, url: String, max_receive: usize) -> Result<Socket> {
    tokio::time::timeout(CONNECT_TIMEOUT, async {
        let stream = UnixStream::connect(path)
            .await
            .with_context(|| format!("connecting to {}", path.display()))?;
        ensure!(
            stream.peer_cred()?.uid() == rustix::process::geteuid().as_raw(),
            "daemon socket belongs to a different user"
        );
        let config = WebSocketConfig::default()
            .max_message_size(Some(max_receive))
            .max_frame_size(Some(max_receive));
        let (socket, _) = client_async_with_config(url, stream, Some(config))
            .await
            .context("WebSocket handshake over Unix socket")?;
        Ok(socket)
    })
    .await
    .context("Unix connection and WebSocket handshake exceeded five seconds")?
}

/// One raw JSON transport. A failed or cancelled I/O operation makes it unusable;
/// drop it and connect again. There are no application-level request timeouts.
pub struct Connection {
    socket: Socket,
    usable: bool,
}

impl Connection {
    /// Connect to a configured chain route, checking its safe route ID and peer UID.
    /// This does not verify chain identity; use [`connect_chain`] for that.
    pub async fn chain(path: &Path, id: &str) -> Result<Self> {
        validate_chain_id(id)?;
        Self::connect(path, format!("ws://localhost/rpc/{id}")).await
    }

    /// Connect to the host discovery route, checking the peer UID.
    pub async fn host(path: &Path) -> Result<Self> {
        Self::connect(path, format!("ws://localhost{DISCOVERY_PATH}")).await
    }

    async fn connect(path: &Path, url: String) -> Result<Self> {
        Ok(Self {
            socket: connect_socket(path, url, MAX_RECEIVE_BYTES).await?,
            usable: true,
        })
    }

    /// Send one JSON text message, limited to 64 KiB. Cancelling the send may leave
    /// a partial write; this connection then refuses further send/receive calls.
    pub async fn send_json(&mut self, value: &Value) -> Result<()> {
        ensure!(self.usable, "connection is unusable; open a new connection");
        let text = serde_json::to_string(value)?;
        ensure!(
            text.len() <= MAX_SEND_BYTES,
            "JSON request exceeds 64 KiB transport limit"
        );
        self.usable = false;
        self.socket
            .send(Message::Text(text.into()))
            .await
            .context("sending JSON")?;
        self.usable = true;
        Ok(())
    }

    /// Receive one JSON text message (at most 8 MiB), or `None` on peer closure.
    /// Ping/pong frames are handled inline. Cancellation is deliberately fail-closed:
    /// a frame or pong may already have been consumed, so reconnect before reuse.
    pub async fn receive_json(&mut self) -> Result<Option<Value>> {
        Ok(self.receive_sized().await?.map(|(value, _)| value))
    }

    async fn receive_sized(&mut self) -> Result<Option<(Value, usize)>> {
        ensure!(self.usable, "connection is unusable; open a new connection");
        self.usable = false;
        while let Some(message) = self.socket.next().await {
            match message.context("reading WebSocket session")? {
                Message::Text(text) => {
                    let value = serde_json::from_str(&text).context("invalid JSON from daemon")?;
                    self.usable = true;
                    return Ok(Some((value, text.len())));
                }
                Message::Close(_) => return Ok(None),
                Message::Ping(_) => self
                    .socket
                    .flush()
                    .await
                    .context("sending WebSocket pong")?,
                Message::Pong(_) => {}
                _ => bail!("unexpected non-text WebSocket message"),
            }
        }
        Ok(None)
    }

    /// Send/flush a close frame, without waiting for a peer close acknowledgment.
    /// This permanently disables send/receive, even if cancelled or unsuccessful.
    /// A cancelled close may not reach the peer; drop the connection to release it.
    pub async fn close(&mut self) -> Result<()> {
        self.usable = false;
        self.socket.close(None).await.context("closing WebSocket")
    }
}

/// Sequential JSON-RPC requests with a bounded, wire-ordered notification backlog.
/// A valid remote RPC error leaves the client usable. Protocol, I/O, backlog, and
/// cancellation errors require a new connection. Dropping an in-flight `call`
/// never allows a later call to consume its stale reply. Apply deadlines externally.
pub struct RpcClient {
    connection: Connection,
    next_id: u64,
    usable: bool,
    notifications: VecDeque<(Value, usize)>,
    notification_bytes: usize,
}

impl RpcClient {
    pub fn new(connection: Connection) -> Self {
        Self {
            connection,
            next_id: 1,
            usable: true,
            notifications: VecDeque::new(),
            notification_bytes: 0,
        }
    }

    /// Call one method. Parameters must be a JSON array or object. Notifications
    /// encountered before its reply are retained, up to 256 envelopes / 8 MiB of
    /// original JSON wire bytes. Overflow is fatal, never silent dropping.
    pub async fn call(&mut self, method: &str, params: Value) -> Result<Value> {
        ensure!(self.usable, "RPC client is unusable; open a new connection");
        ensure!(
            params.is_array() || params.is_object(),
            "RPC params must be an array or object"
        );
        let id = self.next_id;
        self.next_id = id.checked_add(1).context("RPC request ID exhausted")?;
        // Set before the first await: cancellation must leave this client poisoned.
        self.usable = false;
        self.connection
            .send_json(&json!({
                "jsonrpc": "2.0", "id": id, "method": method, "params": params,
            }))
            .await?;
        loop {
            let (mut value, bytes) = self.receive().await?;
            if let Some(response_id) = value.get("id") {
                ensure!(
                    response_id.as_u64() == Some(id),
                    "unmatched RPC response ID: {value}"
                );
                ensure!(
                    value.get("result").is_some() != value.get("error").is_some()
                        && value.get("method").is_none()
                        && value.get("params").is_none(),
                    "invalid RPC response: {value}"
                );
                if let Some(error) = value.get("error") {
                    ensure!(
                        error.is_object()
                            && error["code"].as_i64().is_some()
                            && error["message"].is_string(),
                        "invalid RPC error: {error}"
                    );
                    self.usable = true;
                    bail!("RPC {method} failed: {error}");
                }
                self.usable = true;
                return Ok(value["result"].take());
            }
            validate_notification(&value)?;
            ensure!(
                self.notifications.len() < MAX_NOTIFICATIONS
                    && bytes <= MAX_RECEIVE_BYTES - self.notification_bytes,
                "RPC notification backlog exceeds 256 envelopes or 8 MiB"
            );
            self.notification_bytes += bytes;
            self.notifications.push_back((value, bytes));
        }
    }

    /// Return the oldest notification, waiting if necessary. Unsolicited replies
    /// are fatal. Cancelling while waiting poisons the client just like `call`.
    pub async fn next_notification(&mut self) -> Result<Value> {
        ensure!(self.usable, "RPC client is unusable; open a new connection");
        if let Some((value, bytes)) = self.notifications.pop_front() {
            self.notification_bytes -= bytes;
            return Ok(value);
        }
        self.usable = false;
        let (value, _) = self.receive().await?;
        validate_notification(&value)?;
        self.usable = true;
        Ok(value)
    }

    async fn receive(&mut self) -> Result<(Value, usize)> {
        let (value, bytes) = self
            .connection
            .receive_sized()
            .await?
            .context("daemon disconnected before RPC completed")?;
        ensure!(
            value.is_object() && value["jsonrpc"] == "2.0",
            "invalid JSON-RPC envelope: {value}"
        );
        Ok((value, bytes))
    }

    /// Permanently disable this client and close its transport. Cancellation has
    /// the same caveat as [`Connection::close`]; drop it to release the socket.
    pub async fn close(&mut self) -> Result<()> {
        self.usable = false;
        self.connection.close().await
    }
}

fn validate_notification(value: &Value) -> Result<()> {
    ensure!(
        value.get("id").is_none()
            && value["method"].is_string()
            && value.get("result").is_none()
            && value.get("error").is_none()
            && value
                .get("params")
                .is_none_or(|params| params.is_array() || params.is_object()),
        "invalid or unexpected RPC notification: {value}"
    );
    Ok(())
}

/// Discover configured chains and require the supported host protocol version.
/// This is not a synchronization/readiness check and does not establish trust in
/// a chain: applications should compare discovery against their pinned genesis.
pub async fn discover(path: &Path) -> Result<HostInfo> {
    let mut client = RpcClient::new(Connection::host(path).await?);
    let info: HostInfo = serde_json::from_value(client.call(DISCOVERY_METHOD, json!([])).await?)
        .context("invalid host discovery response")?;
    ensure!(
        info.protocol_version == PROTOCOL_VERSION,
        "unsupported host protocol version: {}",
        info.protocol_version
    );
    client.close().await?;
    Ok(info)
}

/// Open a chain route and verify its actual genesis hash against the descriptor.
/// Does not wait for synchronization; callers should pin the expected genesis
/// independently when trusting a specific chain rather than the local catalog.
pub async fn connect_chain(path: &Path, chain: &ChainInfo) -> Result<RpcClient> {
    let mut client = RpcClient::new(Connection::chain(path, &chain.id).await?);
    let genesis = client.call("chainSpec_v1_genesisHash", json!([])).await?;
    ensure!(
        genesis.as_str() == Some(chain.genesis_hash.as_str()),
        "chain route {} genesis mismatch: expected {}, received {genesis}",
        chain.id,
        chain.genesis_hash
    );
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixListener;
    use tokio_tungstenite::{
        accept_hdr_async,
        tungstenite::handshake::server::{Request, Response},
    };

    fn listener() -> (tempfile::TempDir, std::path::PathBuf, UnixListener) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("rpc.sock");
        let listener = UnixListener::bind(&path).unwrap();
        (directory, path, listener)
    }

    async fn accept(listener: &UnixListener, route: &'static str) -> Socket {
        let (stream, _) = listener.accept().await.unwrap();
        accept_hdr_async(stream, move |request: &Request, response: Response| {
            assert_eq!(request.uri().path(), route);
            Ok(response)
        })
        .await
        .unwrap()
    }

    async fn pair() -> (RpcClient, Socket) {
        let (_directory, path, listener) = listener();
        let (connection, peer) = tokio::join!(
            Connection::chain(&path, "test-chain"),
            accept(&listener, "/rpc/test-chain"),
        );
        (RpcClient::new(connection.unwrap()), peer)
    }

    async fn request(peer: &mut Socket) -> Value {
        let text = peer.next().await.unwrap().unwrap().into_text().unwrap();
        serde_json::from_str(&text).unwrap()
    }

    async fn send(peer: &mut Socket, value: Value) {
        peer.send(Message::Text(value.to_string().into()))
            .await
            .unwrap();
    }

    fn notification(index: u32) -> Value {
        json!({"jsonrpc": "2.0", "method": "event", "params": {"index": index}})
    }

    #[tokio::test]
    async fn interleaved_notifications_survive_remote_errors_in_wire_order() {
        let (mut client, mut peer) = pair().await;
        let stop = json!({"jsonrpc":"2.0", "method":"chainHead_v1_followEvent",
            "params":{"subscription":"s", "result":{"event":"stop"}}});
        tokio::join!(
            async {
                let error = client.call("first", json!([])).await.unwrap_err();
                assert!(error.to_string().contains("remote failure"));
                assert_eq!(client.call("second", json!([])).await.unwrap(), json!(42));
                assert_eq!(client.next_notification().await.unwrap(), notification(1));
                assert_eq!(client.next_notification().await.unwrap(), stop);
                assert_eq!(client.next_notification().await.unwrap(), notification(3));
                assert_eq!(client.notification_bytes, 0);
            },
            async {
                let first = request(&mut peer).await;
                send(&mut peer, notification(1)).await;
                send(&mut peer, stop.clone()).await;
                send(
                    &mut peer,
                    json!({"jsonrpc":"2.0", "id":first["id"],
                "error":{"code":-32000, "message":"remote failure"}}),
                )
                .await;
                let second = request(&mut peer).await;
                send(&mut peer, notification(3)).await;
                send(
                    &mut peer,
                    json!({"jsonrpc":"2.0", "id":second["id"], "result":42}),
                )
                .await;
            }
        );
    }

    #[tokio::test]
    async fn cancelled_call_cannot_consume_a_stale_reply() {
        let (mut client, mut peer) = pair().await;
        let sent;
        {
            let call = client.call("cancelled", json!([]));
            tokio::pin!(call);
            tokio::select! {
                result = &mut call => panic!("call unexpectedly finished: {result:?}"),
                value = request(&mut peer) => sent = value,
            }
        }
        send(
            &mut peer,
            json!({"jsonrpc":"2.0", "id":sent["id"], "result":"stale"}),
        )
        .await;
        assert!(
            client
                .call("later", json!([]))
                .await
                .unwrap_err()
                .to_string()
                .contains("unusable")
        );
        assert!(client.next_notification().await.is_err());
    }

    #[tokio::test]
    async fn cancelled_receive_requires_a_new_connection() {
        let (mut client, _peer) = pair().await;
        {
            let receive = client.connection.receive_json();
            tokio::pin!(receive);
            tokio::select! {
                biased;
                result = &mut receive => panic!("receive unexpectedly finished: {result:?}"),
                _ = std::future::ready(()) => {}
            }
        }
        assert!(client.connection.receive_json().await.is_err());
        assert!(client.connection.send_json(&json!({})).await.is_err());
    }

    #[tokio::test]
    async fn notification_count_limit_is_fatal_without_dropping() {
        let (mut client, mut peer) = pair().await;
        tokio::join!(
            async {
                assert_eq!(client.call("fill", json!([])).await.unwrap(), Value::Null);
                assert_eq!(client.notifications.len(), MAX_NOTIFICATIONS);
                assert!(
                    client
                        .call("overflow", json!([]))
                        .await
                        .unwrap_err()
                        .to_string()
                        .contains("backlog")
                );
                assert_eq!(client.notifications.len(), MAX_NOTIFICATIONS);
                assert!(client.next_notification().await.is_err());
                assert!(client.call("later", json!([])).await.is_err());
            },
            async {
                let first = request(&mut peer).await;
                for index in 0..MAX_NOTIFICATIONS {
                    send(&mut peer, notification(index as u32)).await;
                }
                send(
                    &mut peer,
                    json!({"jsonrpc":"2.0", "id":first["id"], "result":null}),
                )
                .await;
                request(&mut peer).await;
                send(&mut peer, notification(256)).await;
            }
        );
    }

    #[tokio::test]
    async fn notification_byte_limit_counts_original_wire_whitespace() {
        let (mut client, mut peer) = pair().await;
        tokio::join!(
            async {
                assert!(
                    client
                        .call("overflow", json!([]))
                        .await
                        .unwrap_err()
                        .to_string()
                        .contains("backlog")
                );
                assert_eq!(client.notifications.len(), 1);
                assert_eq!(client.notification_bytes, MAX_RECEIVE_BYTES / 2 + 1);
                assert!(client.next_notification().await.is_err());
            },
            async {
                request(&mut peer).await;
                for index in 0..2 {
                    let mut text = notification(index).to_string();
                    text.extend(std::iter::repeat_n(
                        ' ',
                        MAX_RECEIVE_BYTES / 2 + 1 - text.len(),
                    ));
                    peer.send(Message::Text(text.into())).await.unwrap();
                }
            }
        );
    }

    #[tokio::test]
    async fn unmatched_and_malformed_responses_poison_the_client() {
        for response in [
            json!({"jsonrpc":"2.0", "id":999, "result":null}),
            json!({"jsonrpc":"2.0", "id":1, "result":null, "error":{"code":1,"message":"bad"}}),
            json!({"jsonrpc":"2.0", "id":1, "error":{"code":"bad","message":"bad"}}),
            json!({"jsonrpc":"1.0", "id":1, "result":null}),
        ] {
            let (mut client, mut peer) = pair().await;
            tokio::join!(
                async {
                    assert!(client.call("first", json!([])).await.is_err());
                    assert!(
                        client
                            .call("later", json!([]))
                            .await
                            .unwrap_err()
                            .to_string()
                            .contains("unusable")
                    );
                },
                async {
                    request(&mut peer).await;
                    send(&mut peer, response).await;
                }
            );
        }
    }

    #[tokio::test]
    async fn discovery_validates_protocol_version_on_the_host_route() {
        for version in [PROTOCOL_VERSION, PROTOCOL_VERSION + 1] {
            let (_directory, path, listener) = listener();
            let (result, ()) = tokio::join!(discover(&path), async {
                let mut peer = accept(&listener, DISCOVERY_PATH).await;
                let request = request(&mut peer).await;
                assert_eq!(request["method"], DISCOVERY_METHOD);
                assert_eq!(request["params"], json!([]));
                send(
                    &mut peer,
                    json!({"jsonrpc":"2.0", "id":request["id"],
                    "result":{"protocol_version":version, "read_only":true, "chains":[]}}),
                )
                .await;
                // Keep the peer alive until discover closes or drops its transport.
                let _ = peer.next().await;
            });
            if version == PROTOCOL_VERSION {
                let host = result.unwrap();
                assert_eq!(host.protocol_version, PROTOCOL_VERSION);
                assert!(
                    host.content.is_none(),
                    "legacy discovery must not invent content support"
                );
            } else {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("unsupported host protocol")
                );
            }
        }
    }

    #[tokio::test]
    async fn legacy_discovery_without_content_still_allows_chain_rpc() {
        let (_directory, path, listener) = listener();
        let ((), ()) = tokio::join!(
            async {
                let host = discover(&path).await.unwrap();
                assert!(host.content.is_none());
                let mut rpc = connect_chain(&path, &host.chains[0]).await.unwrap();
                assert_eq!(
                    rpc.call("chainSpec_v1_properties", json!([]))
                        .await
                        .unwrap(),
                    json!({"tokenSymbol":"UNIT"})
                );
                rpc.close().await.unwrap();
            },
            async {
                let mut peer = accept(&listener, DISCOVERY_PATH).await;
                let discovery = request(&mut peer).await;
                send(
                    &mut peer,
                    json!({
                        "jsonrpc":"2.0", "id":discovery["id"], "result":{
                            "protocol_version":PROTOCOL_VERSION, "read_only":true,
                            "chains":[{
                                "id":"legacy", "name":"Legacy", "genesis_hash":"0x11",
                                "relay_chain":null, "para_id":null, "properties":{},
                                "rpc_methods":["chainSpec_v1_genesisHash","chainSpec_v1_properties"]
                            }]
                        }
                    }),
                )
                .await;
                let _ = peer.next().await;
                let mut peer = accept(&listener, "/rpc/legacy").await;
                let genesis = request(&mut peer).await;
                send(
                    &mut peer,
                    json!({"jsonrpc":"2.0","id":genesis["id"],"result":"0x11"}),
                )
                .await;
                let properties = request(&mut peer).await;
                assert_eq!(properties["method"], "chainSpec_v1_properties");
                send(
                    &mut peer,
                    json!({"jsonrpc":"2.0","id":properties["id"],"result":{"tokenSymbol":"UNIT"}}),
                )
                .await;
                let _ = peer.next().await;
            },
        );
    }

    #[tokio::test]
    async fn connect_chain_checks_the_selected_routes_genesis() {
        for expected in ["0x11", "0x22"] {
            let (_directory, path, listener) = listener();
            let chain = ChainInfo {
                id: "asset-hub".into(),
                name: "Asset Hub".into(),
                genesis_hash: expected.into(),
                relay_chain: Some("relay".into()),
                para_id: Some(1000),
                properties: json!({}),
                rpc_methods: Vec::new(),
            };
            let (result, ()) = tokio::join!(connect_chain(&path, &chain), async {
                let mut peer = accept(&listener, "/rpc/asset-hub").await;
                let request = request(&mut peer).await;
                assert_eq!(request["method"], "chainSpec_v1_genesisHash");
                assert_eq!(request["params"], json!([]));
                send(
                    &mut peer,
                    json!({"jsonrpc":"2.0", "id":request["id"], "result":"0x22"}),
                )
                .await;
            });
            match result {
                Ok(_) => assert_eq!(expected, "0x22"),
                Err(error) => {
                    assert_eq!(expected, "0x11");
                    assert!(error.to_string().contains("genesis mismatch"));
                }
            }
        }
    }
}
