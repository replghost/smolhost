// SPDX-License-Identifier: GPL-3.0-or-later

use std::{
    fs, io,
    os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json, value::RawValue};
use tokio::net::{UnixListener, UnixStream};
use tokio_tungstenite::{
    accept_hdr_async_with_config,
    tungstenite::{
        Message,
        handshake::server::{ErrorResponse, Request, Response},
        protocol::WebSocketConfig,
    },
};

use crate::{
    discovery::{DISCOVERY_METHOD, DISCOVERY_PATH, HostInfo},
    engine::{Session, SharedEngine},
};

const IO_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_REQUEST_BYTES: usize = 64 * 1024;
pub(crate) const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// Immutable discovery result, serialized once before accepting applications.
pub struct HostCatalog {
    result: Box<RawValue>,
}

impl HostCatalog {
    pub fn new(info: &HostInfo) -> Result<Self> {
        let result = serde_json::value::to_raw_value(info)?;
        // Reserve space for any request ID permitted by the incoming limit.
        ensure!(
            result.get().len() + MAX_REQUEST_BYTES + 64 <= MAX_RESPONSE_BYTES,
            "host catalog exceeds transport limit"
        );
        Ok(Self { result })
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Route {
    Host,
    Chain(usize),
    Content(usize),
}

pub struct BoundSocket {
    pub listener: UnixListener,
    path: PathBuf,
    owned_identity: Option<(u64, u64)>,
}

impl BoundSocket {
    pub fn bind(path: PathBuf) -> Result<Self> {
        ensure!(path.is_absolute(), "socket path must be absolute");
        let parent = path.parent().context("socket has no parent directory")?;
        match fs::DirBuilder::new().mode(0o700).create(parent) {
            Ok(()) => (),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (),
            Err(error) => return Err(error).context("creating private socket directory"),
        }
        validate_directory(parent)?;
        // Never unlink an existing endpoint: another daemon may own it.
        let listener = UnixListener::bind(&path)
            .context("binding socket (existing endpoints are not replaced)")?;
        let meta = fs::symlink_metadata(&path)?;
        let bound = Self {
            listener,
            path,
            owned_identity: Some((meta.dev(), meta.ino())),
        };
        fs::set_permissions(&bound.path, fs::Permissions::from_mode(0o600))?;
        Ok(bound)
    }

    /// Adopt the supervisor's listener without taking ownership of its pathname.
    pub fn from_inherited(
        path: PathBuf,
        listener: std::os::unix::net::UnixListener,
    ) -> Result<Self> {
        ensure!(path.is_absolute(), "socket path must be absolute");
        validate_directory(path.parent().context("socket has no parent directory")?)?;
        ensure!(
            listener.local_addr()?.as_pathname() == Some(path.as_path()),
            "inherited listener does not match the configured socket path"
        );
        let meta = fs::symlink_metadata(&path)?;
        ensure!(
            meta.file_type().is_socket()
                && meta.uid() == rustix::process::geteuid().as_raw()
                && meta.mode() & 0o777 == 0o600,
            "inherited socket must be owned by this user with mode 0600"
        );
        listener.set_nonblocking(true)?;
        Ok(Self {
            listener: UnixListener::from_std(listener)?,
            path,
            owned_identity: None,
        })
    }
}

fn validate_directory(path: &Path) -> Result<()> {
    let meta = fs::symlink_metadata(path)?;
    ensure!(
        meta.is_dir()
            && meta.uid() == rustix::process::geteuid().as_raw()
            && meta.mode() & 0o077 == 0,
        "socket directory must be a real directory owned by this user with mode 0700"
    );
    Ok(())
}

impl Drop for BoundSocket {
    fn drop(&mut self) {
        // Leave a replacement endpoint alone if our pathname changed underneath us.
        if let Ok(meta) = fs::symlink_metadata(&self.path) {
            if Some((meta.dev(), meta.ino())) == self.owned_identity {
                let _ = fs::remove_file(&self.path);
            }
        }
    }
}

pub async fn serve(
    stream: UnixStream,
    chains: &[String],
    engine: SharedEngine,
    catalog: Arc<HostCatalog>,
) -> Result<()> {
    ensure!(
        stream.peer_cred()?.uid() == rustix::process::geteuid().as_raw(),
        "peer UID mismatch"
    );
    let mut selected = None;
    let callback =
        |request: &Request, response: Response| -> std::result::Result<Response, ErrorResponse> {
            selected = Some(select_route(request, chains)?);
            Ok(response)
        };
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_REQUEST_BYTES))
        .max_frame_size(Some(MAX_REQUEST_BYTES));
    let mut socket = tokio::time::timeout(
        IO_TIMEOUT,
        accept_hdr_async_with_config(stream, callback, Some(config)),
    )
    .await
    .context("WebSocket handshake timed out")??;
    let route = selected.context("WebSocket handshake did not select a route")?;
    if let Route::Content(index) = route {
        return crate::content::serve(socket, engine, &chains[index]).await;
    }
    let mut session = match route {
        Route::Host => None,
        Route::Chain(index) => Some(Session::open(engine, &chains[index])?),
        Route::Content(_) => unreachable!("content is served separately"),
    };
    loop {
        let outgoing = tokio::select! {
            message = socket.next() => {
                match message {
                    None | Some(Ok(Message::Close(_))) => return Ok(()),
                    Some(Err(error)) => return Err(error.into()),
                    Some(Ok(Message::Text(text))) => {
                        if let Some(session) = &session {
                            match validate_request(&text) {
                                Ok(()) => { session.request(text.to_string())?; continue; }
                                Err(Some(error)) => Message::Text(error.to_string().into()),
                                Err(None) => continue,
                            }
                        } else {
                            match host_response(&text, &catalog) {
                                Some(response) => Message::Text(response.into()),
                                None => continue,
                            }
                        }
                    }
                    Some(Ok(Message::Ping(data))) => Message::Pong(data),
                    Some(Ok(Message::Pong(_))) => continue,
                    Some(Ok(_)) => bail!("only JSON text messages are accepted"),
                }
            }
            response = async {
                match &mut session {
                    Some(session) => session.responses.next().await,
                    None => std::future::pending().await,
                }
            } => {
                let response = response.context("engine response stream closed")?;
                ensure!(response.len() <= MAX_RESPONSE_BYTES, "RPC response exceeds transport limit");
                Message::Text(filter_methods(response).into())
            }
        };
        ensure!(
            outgoing.len() <= MAX_RESPONSE_BYTES,
            "RPC response exceeds transport limit"
        );
        // No unbounded forwarding queue. A blocked writer loses its session, not events.
        tokio::time::timeout(IO_TIMEOUT, socket.send(outgoing))
            .await
            .context("client is not consuming responses")??;
    }
}

fn select_route(request: &Request, chains: &[String]) -> std::result::Result<Route, ErrorResponse> {
    let selected = if request.uri().path() == DISCOVERY_PATH {
        Some(Route::Host)
    } else {
        [("/rpc/", false), ("/content/", true)]
            .into_iter()
            .find_map(|(prefix, content)| {
                request
                    .uri()
                    .path()
                    .strip_prefix(prefix)
                    .and_then(|id| chains.iter().position(|chain| chain == id))
                    .map(|index| {
                        if content {
                            Route::Content(index)
                        } else {
                            Route::Chain(index)
                        }
                    })
            })
    }
    .filter(|_| request.uri().query().is_none())
    .ok_or_else(|| {
        tokio_tungstenite::tungstenite::http::Response::builder()
            .status(404)
            .body(Some("unknown endpoint".into()))
            .unwrap()
    })?;
    if request.headers().contains_key("origin") {
        return Err(tokio_tungstenite::tungstenite::http::Response::builder()
            .status(403)
            .body(Some("browser origins are not supported".into()))
            .unwrap());
    }
    Ok(selected)
}

pub(crate) fn allowed_method(method: &str) -> bool {
    matches!(
        method,
        "rpc_methods"
            | "chainSpec_v1_chainName"
            | "chainSpec_v1_genesisHash"
            | "chainSpec_v1_properties"
            | "chainHead_v1_follow"
            | "chainHead_v1_unfollow"
            | "chainHead_v1_header"
            | "chainHead_v1_body"
            | "chainHead_v1_storage"
            | "chainHead_v1_call"
            | "chainHead_v1_continue"
            | "chainHead_v1_stopOperation"
            | "chainHead_v1_unpin"
    )
}

fn parse_request(text: &str) -> std::result::Result<Value, Option<Value>> {
    let request: Value = serde_json::from_str(text)
        .map_err(|_| Some(rpc_error(Value::Null, -32700, "Parse error")))?;
    let id = request.get("id");
    if request.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || request.get("method").and_then(Value::as_str).is_none()
        || id.is_some_and(|id| !(id.is_string() || id.is_number() || id.is_null()))
    {
        return Err(Some(rpc_error(Value::Null, -32600, "Invalid Request")));
    }
    Ok(request)
}

fn validate_request(text: &str) -> std::result::Result<(), Option<Value>> {
    let request = parse_request(text)?;
    let id = request.get("id");
    if !allowed_method(request["method"].as_str().unwrap()) {
        return Err(id.map(|id| {
            rpc_error(
                id.clone(),
                -32601,
                "Method not available on this read-only host",
            )
        }));
    }
    Ok(())
}

fn host_response(text: &str, catalog: &HostCatalog) -> Option<String> {
    let request = match parse_request(text) {
        Ok(request) => request,
        Err(error) => return error.map(|error| error.to_string()),
    };
    // Valid notifications never receive a response, including method/params errors.
    let id = request.get("id")?;
    let method = request["method"].as_str().unwrap();
    if !matches!(method, "rpc_methods" | DISCOVERY_METHOD) {
        return Some(rpc_error(id.clone(), -32601, "Method not found").to_string());
    }
    if request
        .get("params")
        .is_some_and(|params| !params.as_array().is_some_and(Vec::is_empty))
    {
        return Some(rpc_error(id.clone(), -32602, "Invalid params").to_string());
    }
    let result = if method == DISCOVERY_METHOD {
        catalog.result.get()
    } else {
        r#"{"methods":["rpc_methods","host_discover"]}"#
    };
    Some(format!(
        r#"{{"jsonrpc":"2.0","id":{id},"result":{result}}}"#
    ))
}

fn rpc_error(id: Value, code: i32, message: &str) -> Value {
    json!({"jsonrpc":"2.0", "id":id, "error":{"code":code, "message":message}})
}

fn filter_methods(response: String) -> String {
    // Keep rpc_methods truthful without deserializing every large storage response.
    if response.contains("\"methods\"") {
        if let Ok(mut value) = serde_json::from_str::<Value>(&response) {
            if let Some(methods) = value
                .pointer_mut("/result/methods")
                .and_then(Value::as_array_mut)
            {
                methods.retain(|method| method.as_str().is_some_and(allowed_method));
                return value.to_string();
            }
        }
    }
    response
}

/// Require a regular, size-bounded chain-spec file before loading it into memory.
pub fn read_chain_spec(path: &Path) -> Result<String> {
    use std::io::Read;
    let file = fs::File::open(path).context("opening chain specification")?;
    ensure!(
        file.metadata()?.is_file(),
        "chain specification must be a regular file"
    );
    let mut specification = String::new();
    file.take(16 * 1024 * 1024 + 1)
        .read_to_string(&mut specification)?;
    ensure!(
        specification.len() <= 16 * 1024 * 1024,
        "chain specification exceeds 16 MiB"
    );
    Ok(specification)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_route_is_private_exact_and_does_not_expose_bitswap_rpc() {
        let chains = vec!["bulletin".to_owned()];
        let request = Request::builder()
            .uri("/content/bulletin")
            .body(())
            .unwrap();
        assert_eq!(select_route(&request, &chains).unwrap(), Route::Content(0));
        for path in [
            "/content/",
            "/content/missing",
            "/content/bulletin/",
            "/content/bulletin?x=1",
            "/content/%62ulletin",
        ] {
            let request = Request::builder().uri(path).body(()).unwrap();
            assert_eq!(select_route(&request, &chains).unwrap_err().status(), 404);
        }
        for origin in ["https://example.com", "file://", "null", ""] {
            let request = Request::builder()
                .uri("/content/bulletin")
                .header("Origin", origin)
                .body(())
                .unwrap();
            assert_eq!(select_route(&request, &chains).unwrap_err().status(), 403);
        }
        for method in [
            "bitswap_unstable_get",
            "bitswap_unstable_stream",
            "bitswap_unstable_unstream",
        ] {
            assert!(!allowed_method(method));
        }
    }

    fn empty_catalog() -> HostCatalog {
        HostCatalog::new(&HostInfo {
            protocol_version: crate::discovery::PROTOCOL_VERSION,
            read_only: true,
            chains: Vec::new(),
        })
        .unwrap()
    }

    #[test]
    fn host_route_is_exact_and_rejects_browser_origins_without_chains() {
        let request = Request::builder().uri("/host").body(()).unwrap();
        assert_eq!(select_route(&request, &[]).unwrap(), Route::Host);
        for path in [
            "/host/",
            "/Host",
            "/%68ost",
            "/host?",
            "/host?chain=paseo",
            "/host/../rpc/paseo",
            "/rpc/host",
        ] {
            let request = Request::builder().uri(path).body(()).unwrap();
            assert_eq!(select_route(&request, &[]).unwrap_err().status(), 404);
        }
        for origin in ["https://example.com", "file://", "null", ""] {
            let request = Request::builder()
                .uri("/host")
                .header("Origin", origin)
                .body(())
                .unwrap();
            assert_eq!(select_route(&request, &[]).unwrap_err().status(), 403);
        }
    }

    #[test]
    fn host_errors_preserve_only_valid_request_ids() {
        let catalog = empty_catalog();
        for (text, code, id) in [
            ("{", -32700, Value::Null),
            ("[]", -32600, Value::Null),
            (
                r#"[{"jsonrpc":"2.0","id":7,"method":"host_discover"}]"#,
                -32600,
                Value::Null,
            ),
            (
                r#"{"jsonrpc":"1.0","id":7,"method":"host_discover"}"#,
                -32600,
                Value::Null,
            ),
            (r#"{"jsonrpc":"2.0","id":7}"#, -32600, Value::Null),
            (
                r#"{"jsonrpc":"2.0","id":true,"method":"host_discover"}"#,
                -32600,
                Value::Null,
            ),
            (
                r#"{"jsonrpc":"2.0","id":[],"method":"host_discover"}"#,
                -32600,
                Value::Null,
            ),
            (
                r#"{"jsonrpc":"2.0","id":{},"method":"host_discover"}"#,
                -32600,
                Value::Null,
            ),
            (
                r#"{"jsonrpc":"2.0","id":"client","method":"chainHead_v1_follow"}"#,
                -32601,
                json!("client"),
            ),
            (
                r#"{"jsonrpc":"2.0","id":7,"method":"author_submitExtrinsic"}"#,
                -32601,
                json!(7),
            ),
            (
                r#"{"jsonrpc":"2.0","id":null,"method":"unknown"}"#,
                -32601,
                Value::Null,
            ),
        ] {
            let response: Value =
                serde_json::from_str(&host_response(text, &catalog).unwrap()).unwrap();
            assert_eq!(response["error"]["code"], code, "{text}");
            assert_eq!(response["id"], id, "{text}");
        }
    }

    #[test]
    fn host_methods_require_no_arguments_and_keep_chain_gate_separate() {
        let catalog = empty_catalog();
        for method in [DISCOVERY_METHOD, "rpc_methods"] {
            for params in [json!({}), Value::Null, json!([1]), json!(false), json!("")] {
                let request = json!({"jsonrpc":"2.0","id":"args","method":method,"params":params});
                let response: Value =
                    serde_json::from_str(&host_response(&request.to_string(), &catalog).unwrap())
                        .unwrap();
                assert_eq!(response["error"]["code"], -32602);
                assert_eq!(response["id"], "args");
            }
            for params in [None, Some(json!([]))] {
                let mut request = json!({"jsonrpc":"2.0","id":null,"method":method});
                if let Some(params) = params {
                    request["params"] = params;
                }
                let response: Value =
                    serde_json::from_str(&host_response(&request.to_string(), &catalog).unwrap())
                        .unwrap();
                assert!(response.get("error").is_none());
                assert!(response["id"].is_null());
                if method == "rpc_methods" {
                    assert_eq!(
                        response["result"]["methods"],
                        json!(["rpc_methods", DISCOVERY_METHOD])
                    );
                } else {
                    assert_eq!(
                        response["result"]["protocol_version"],
                        crate::discovery::PROTOCOL_VERSION
                    );
                    assert_eq!(response["result"]["read_only"], true);
                }
            }
        }
        let error = validate_request(r#"{"jsonrpc":"2.0","id":7,"method":"host_discover"}"#)
            .unwrap_err()
            .unwrap();
        assert_eq!(error["error"]["code"], -32601);
        assert_eq!(error["id"], 7);
    }

    #[test]
    fn host_valid_notifications_never_receive_replies() {
        let catalog = empty_catalog();
        for method in [
            DISCOVERY_METHOD,
            "rpc_methods",
            "author_submitExtrinsic",
            "unknown",
        ] {
            for params in [None, Some(json!([])), Some(json!([1])), Some(Value::Null)] {
                let mut request = json!({"jsonrpc":"2.0","method":method});
                if let Some(params) = params {
                    request["params"] = params;
                }
                assert!(host_response(&request.to_string(), &catalog).is_none());
            }
        }
        // Omitting an ID does not make a malformed request a valid notification.
        let response: Value = serde_json::from_str(
            &host_response(r#"{"jsonrpc":"2.0","method":false}"#, &catalog).unwrap(),
        )
        .unwrap();
        assert_eq!(response["error"]["code"], -32600);
        assert!(response["id"].is_null());
    }

    #[test]
    fn catalog_rejects_oversized_metadata_before_serving() {
        let info = HostInfo {
            protocol_version: crate::discovery::PROTOCOL_VERSION,
            read_only: true,
            chains: vec![crate::discovery::ChainInfo {
                id: "large".into(),
                name: "large".into(),
                genesis_hash: format!("0x{}", "00".repeat(32)),
                relay_chain: None,
                para_id: None,
                properties: json!({"large": "x".repeat(MAX_RESPONSE_BYTES)}),
                rpc_methods: Vec::new(),
            }],
        };
        assert!(HostCatalog::new(&info).is_err());
    }

    #[test]
    fn handshake_selects_only_configured_chain_routes() {
        let chains = vec!["paseo".to_owned(), "asset-hub-paseo".to_owned()];
        for (index, chain) in chains.iter().enumerate() {
            let request = Request::builder()
                .uri(format!("/rpc/{chain}"))
                .body(())
                .unwrap();
            assert_eq!(
                select_route(&request, &chains).unwrap(),
                Route::Chain(index)
            );
        }
        let request = Request::builder()
            .uri("/rpc/asset-hub-paseo")
            .body(())
            .unwrap();
        assert_eq!(
            select_route(&request, &chains[..1]).unwrap_err().status(),
            404
        );
        assert_eq!(select_route(&request, &[]).unwrap_err().status(), 404);
    }

    #[test]
    fn handshake_rejects_route_variants_and_queries() {
        let chains = vec!["paseo".to_owned(), "asset-hub-paseo".to_owned()];
        for path in [
            "/rpc/unknown",
            "/rpc/Paseo",
            "/rpc/",
            "/rpc/paseo/",
            "/rpc/paseo/../asset-hub-paseo",
            "/rpc/%70aseo",
            "/rpc/asset-hub-paseo%2F",
            "/other/paseo",
            "/rpc/paseo?",
            "/rpc/paseo?chain=asset-hub-paseo",
            "/rpc/asset-hub-paseo?chain=paseo",
        ] {
            let request = Request::builder().uri(path).body(()).unwrap();
            assert_eq!(select_route(&request, &chains).unwrap_err().status(), 404);
        }
    }

    #[test]
    fn handshake_rejects_every_browser_origin_on_configured_routes() {
        let chains = vec!["paseo".to_owned(), "asset-hub-paseo".to_owned()];
        for chain in &chains {
            for origin in ["https://example.com", "file://", "null", ""] {
                let request = Request::builder()
                    .uri(format!("/rpc/{chain}"))
                    .header("Origin", origin)
                    .body(())
                    .unwrap();
                assert_eq!(select_route(&request, &chains).unwrap_err().status(), 403);
            }
        }
        let request = Request::builder()
            .uri("/rpc/unknown")
            .header("Origin", "https://example.com")
            .body(())
            .unwrap();
        assert_eq!(select_route(&request, &chains).unwrap_err().status(), 404);
    }

    #[tokio::test]
    async fn second_bind_does_not_replace_live_socket() {
        let directory = tempfile::tempdir().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let path = directory.path().join("rpc.sock");
        let first = BoundSocket::bind(path.clone()).unwrap();
        let inode = fs::symlink_metadata(&path).unwrap().ino();
        assert!(BoundSocket::bind(path.clone()).is_err());
        assert_eq!(fs::symlink_metadata(&path).unwrap().ino(), inode);
        assert!(fs::symlink_metadata(&path).unwrap().file_type().is_socket());
        assert_eq!(fs::symlink_metadata(&path).unwrap().mode() & 0o777, 0o600);
        let _connection = UnixStream::connect(&path).await.unwrap();
        drop(first);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn inherited_listener_leaves_supervisor_endpoint_usable() {
        let directory = tempfile::tempdir().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let path = directory.path().join("rpc.sock");
        let supervisor = std::os::unix::net::UnixListener::bind(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let inherited =
            BoundSocket::from_inherited(path.clone(), supervisor.try_clone().unwrap()).unwrap();
        let connection = UnixStream::connect(&path).await.unwrap();
        let _accepted = inherited.listener.accept().await.unwrap();
        drop(connection);
        drop(inherited);
        // A replacement daemon must be able to accept on the same supervisor socket.
        let replacement = BoundSocket::from_inherited(path.clone(), supervisor).unwrap();
        let _connection = UnixStream::connect(&path).await.unwrap();
        let _accepted = replacement.listener.accept().await.unwrap();
        drop(replacement);
        assert!(path.exists());
    }

    #[tokio::test]
    async fn inherited_listener_rejects_wrong_path_and_exposed_permissions() {
        let directory = tempfile::tempdir().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let path = directory.path().join("rpc.sock");
        let supervisor = std::os::unix::net::UnixListener::bind(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(
            BoundSocket::from_inherited(
                directory.path().join("other.sock"),
                supervisor.try_clone().unwrap()
            )
            .is_err()
        );
        fs::set_permissions(&path, fs::Permissions::from_mode(0o666)).unwrap();
        assert!(
            BoundSocket::from_inherited(path.clone(), supervisor.try_clone().unwrap()).is_err()
        );
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o755)).unwrap();
        assert!(BoundSocket::from_inherited(path.clone(), supervisor).is_err());
        assert!(path.exists());
    }

    #[tokio::test]
    async fn shared_directory_and_symlink_are_rejected() {
        let directory = tempfile::tempdir().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o755)).unwrap();
        let path = directory.path().join("rpc.sock");
        assert!(BoundSocket::bind(path.clone()).is_err());
        assert!(!path.exists());
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let alias = directory.path().join("alias");
        std::os::unix::fs::symlink(directory.path(), &alias).unwrap();
        assert!(BoundSocket::bind(alias.join("rpc.sock")).is_err());
    }

    #[test]
    fn write_methods_cannot_bypass_read_only_gate() {
        let submit =
            r#"{"jsonrpc":"2.0","id":7,"method":"author_submitExtrinsic","params":["0x00"]}"#;
        let error = validate_request(submit).unwrap_err().unwrap();
        assert_eq!(error["error"]["code"], -32601);
        assert_eq!(error["id"], 7);
        assert!(matches!(
            validate_request(r#"{"jsonrpc":"2.0","method":"author_submitExtrinsic"}"#),
            Err(None)
        ));
        let batch = format!("[{submit}]");
        assert_eq!(
            validate_request(&batch).unwrap_err().unwrap()["error"]["code"],
            -32600
        );
        assert_eq!(
            validate_request("{").unwrap_err().unwrap()["error"]["code"],
            -32700
        );
        assert!(
            validate_request(
                r#"{"jsonrpc":"2.0","id":7,"method":"chainHead_v1_follow","params":[false]}"#
            )
            .is_ok()
        );
    }

    #[test]
    fn discovery_does_not_advertise_blocked_methods() {
        let response = json!({"jsonrpc":"2.0","id":4,"result":{"methods":[
            "rpc_methods", "chainHead_v1_follow", "author_submitExtrinsic", "sudo_unstable_p2pDiscover"
        ]}}).to_string();
        let filtered: Value = serde_json::from_str(&filter_methods(response)).unwrap();
        assert_eq!(
            filtered["result"]["methods"],
            json!(["rpc_methods", "chainHead_v1_follow"])
        );
        assert_eq!(filtered["id"], 4);
    }
}
