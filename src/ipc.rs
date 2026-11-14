// SPDX-License-Identifier: GPL-3.0-or-later

use std::{
    fs, io,
    os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::{UnixListener, UnixStream};
use tokio_tungstenite::{
    accept_hdr_async_with_config,
    tungstenite::{
        Message,
        handshake::server::{ErrorResponse, Request, Response},
        protocol::WebSocketConfig,
    },
};

use crate::engine::{Session, SharedEngine};

const IO_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_REQUEST_BYTES: usize = 64 * 1024;
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

pub struct BoundSocket {
    pub listener: UnixListener,
    path: PathBuf,
    identity: (u64, u64),
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
        let meta = fs::symlink_metadata(parent)?;
        ensure!(
            meta.is_dir()
                && meta.uid() == rustix::process::geteuid().as_raw()
                && meta.mode() & 0o077 == 0,
            "socket directory must be a real directory owned by this user with mode 0700"
        );
        // Never unlink an existing endpoint: another daemon may own it.
        let listener = UnixListener::bind(&path)
            .context("binding socket (existing endpoints are not replaced)")?;
        let meta = fs::symlink_metadata(&path)?;
        let bound = Self {
            listener,
            path,
            identity: (meta.dev(), meta.ino()),
        };
        fs::set_permissions(&bound.path, fs::Permissions::from_mode(0o600))?;
        Ok(bound)
    }
}

impl Drop for BoundSocket {
    fn drop(&mut self) {
        // Leave a replacement endpoint alone if our pathname changed underneath us.
        if let Ok(meta) = fs::symlink_metadata(&self.path) {
            if (meta.dev(), meta.ino()) == self.identity {
                let _ = fs::remove_file(&self.path);
            }
        }
    }
}

pub async fn serve(stream: UnixStream, chain: &str, engine: SharedEngine) -> Result<()> {
    ensure!(
        stream.peer_cred()?.uid() == rustix::process::geteuid().as_raw(),
        "peer UID mismatch"
    );
    let expected_path = format!("/rpc/{chain}");
    let callback =
        |request: &Request, response: Response| -> std::result::Result<Response, ErrorResponse> {
            if request.uri().path() != expected_path || request.uri().query().is_some() {
                return Err(tokio_tungstenite::tungstenite::http::Response::builder()
                    .status(404)
                    .body(Some("unknown chain endpoint".into()))
                    .unwrap());
            }
            if request.headers().contains_key("origin") {
                return Err(tokio_tungstenite::tungstenite::http::Response::builder()
                    .status(403)
                    .body(Some("browser origins are not supported".into()))
                    .unwrap());
            }
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
    let mut session = Session::open(engine)?;
    loop {
        let outgoing = tokio::select! {
            message = socket.next() => {
                match message {
                    None | Some(Ok(Message::Close(_))) => return Ok(()),
                    Some(Err(error)) => return Err(error.into()),
                    Some(Ok(Message::Text(text))) => {
                        match validate_request(&text) {
                            Ok(()) => { session.request(text.to_string())?; continue; }
                            Err(Some(error)) => Message::Text(error.to_string().into()),
                            Err(None) => continue,
                        }
                    }
                    Some(Ok(Message::Ping(data))) => Message::Pong(data),
                    Some(Ok(Message::Pong(_))) => continue,
                    Some(Ok(_)) => bail!("only JSON text messages are accepted"),
                }
            }
            response = session.responses.next() => {
                let response = response.context("engine response stream closed")?;
                ensure!(response.len() <= MAX_RESPONSE_BYTES, "RPC response exceeds transport limit");
                Message::Text(filter_methods(response).into())
            }
        };
        // No unbounded forwarding queue. A blocked writer loses its session, not events.
        tokio::time::timeout(IO_TIMEOUT, socket.send(outgoing))
            .await
            .context("client is not consuming responses")??;
    }
}

fn allowed_method(method: &str) -> bool {
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

fn validate_request(text: &str) -> std::result::Result<(), Option<Value>> {
    let request: Value = serde_json::from_str(text)
        .map_err(|_| Some(rpc_error(Value::Null, -32700, "Parse error")))?;
    let id = request.get("id");
    if request.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || request.get("method").and_then(Value::as_str).is_none()
        || id.is_some_and(|id| !(id.is_string() || id.is_number() || id.is_null()))
    {
        return Err(Some(rpc_error(Value::Null, -32600, "Invalid Request")));
    }
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
    use std::os::unix::fs::FileTypeExt;

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
