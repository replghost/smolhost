// SPDX-License-Identifier: GPL-3.0-or-later

use std::{
    collections::{HashSet, VecDeque},
    io::{self, Write},
    path::PathBuf,
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use clap::{Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::UnixStream;
use tokio_tungstenite::{WebSocketStream, client_async, tungstenite::Message};

const NUMBER_KEY: &str = "0x26aa394eea5630e07c48ae0c9558cef702a5c1b19ab7a04f536c519aca4983ac";
type Socket = WebSocketStream<UnixStream>;

#[derive(Parser)]
#[command(about = "Read-only diagnostics for a local polkadot-host daemon")]
struct Args {
    #[arg(long, global = true)]
    socket: Option<PathBuf>,
    #[arg(long)]
    chain: String,
    /// Total deadline, including connection and synchronization.
    #[arg(long, global = true, default_value_t = 240, value_parser = clap::value_parser!(u64).range(1..))]
    timeout_secs: u64,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Send one JSON-RPC payload; stream JSON lines until disconnect or Ctrl-C.
    Rpc { json: String },
    /// Read System.Number initially and after each finalized notification.
    Probe {
        #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u32).range(1..))]
        finalizations: u32,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let raw_rpc = matches!(&args.command, Command::Rpc { .. });
    tokio::select! {
        result = tokio::time::timeout(Duration::from_secs(args.timeout_secs), run(&args)) => {
            result.context("total timeout expired before the command completed")?
        }
        signal = tokio::signal::ctrl_c() => {
            signal.context("installing Ctrl-C handler")?;
            if raw_rpc {
                Ok(())
            } else {
                bail!("probe interrupted before completion")
            }
        }
    }
}

async fn run(args: &Args) -> Result<()> {
    ensure!(
        !args.chain.is_empty()
            && args
                .chain
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b)),
        "chain must be a nonempty URL path component using letters, digits, '-' or '_'"
    );
    let path = match &args.socket {
        Some(path) => path.clone(),
        None => polkadot_host::default_socket_path()?,
    };
    let stream = UnixStream::connect(&path)
        .await
        .with_context(|| format!("connecting to {}", path.display()))?;
    ensure!(
        stream.peer_cred()?.uid() == rustix::process::geteuid().as_raw(),
        "daemon socket belongs to a different user"
    );
    let (socket, _) = client_async(format!("ws://localhost/rpc/{}", args.chain), stream)
        .await
        .context("WebSocket handshake over Unix socket")?;
    match &args.command {
        Command::Rpc { json } => raw_rpc(socket, json).await,
        Command::Probe { finalizations } => probe(socket, *finalizations).await,
    }
}

fn emit(value: &Value) -> Result<()> {
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, value)?;
    writeln!(stdout)?;
    stdout.flush()?;
    Ok(())
}

async fn receive(socket: &mut Socket) -> Result<Option<Value>> {
    while let Some(message) = socket.next().await {
        match message.context("reading WebSocket session")? {
            Message::Text(text) => {
                return Ok(Some(
                    serde_json::from_str(&text).context("invalid JSON from daemon")?,
                ));
            }
            Message::Close(_) => return Ok(None),
            // Tungstenite queues pong frames automatically; flush them promptly.
            Message::Ping(_) => socket.flush().await.context("sending WebSocket pong")?,
            Message::Pong(_) => {}
            _ => bail!("unexpected non-text WebSocket message"),
        }
    }
    Ok(None)
}

async fn raw_rpc(mut socket: Socket, payload: &str) -> Result<()> {
    let value: Value = serde_json::from_str(payload).context("invalid JSON RPC payload")?;
    socket.send(Message::Text(value.to_string().into())).await?;
    while let Some(value) = receive(&mut socket).await? {
        emit(&value)?;
    }
    Ok(())
}

// Requests are sequential. Notifications arriving while a response or storage
// operation is pending are retained in wire order, never discarded.
struct Rpc {
    socket: Socket,
    next_id: u64,
    events: VecDeque<Value>,
}

impl Rpc {
    async fn receive(&mut self) -> Result<Value> {
        let value = receive(&mut self.socket)
            .await?
            .context("daemon disconnected before probe completed")?;
        ensure!(
            value["jsonrpc"] == "2.0",
            "invalid JSON-RPC envelope: {value}"
        );
        if let Some(error) = value.get("error") {
            bail!("JSON-RPC error: {error}");
        }
        ensure!(
            value["params"]["result"]["event"] != "stop",
            "follow subscription stopped; probe cannot continue"
        );
        Ok(value)
    }

    async fn call(&mut self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id;
        self.next_id += 1;
        self.socket
            .send(Message::Text(
                json!({
                    "jsonrpc": "2.0", "id": id, "method": method, "params": params,
                })
                .to_string()
                .into(),
            ))
            .await?;
        loop {
            let value = self
                .receive()
                .await
                .with_context(|| format!("calling {method}"))?;
            if value.get("id").is_some() {
                ensure!(
                    value["id"] == id,
                    "unexpected response ID while calling {method}: {value}"
                );
                return value
                    .get("result")
                    .cloned()
                    .context("RPC response has no result");
            }
            self.events.push_back(value);
        }
    }

    async fn event(&mut self, subscription: &str) -> Result<Value> {
        let value = match self.events.pop_front() {
            Some(value) => value,
            None => self.receive().await?,
        };
        ensure!(
            value["method"] == "chainHead_v1_followEvent",
            "unexpected notification: {value}"
        );
        ensure!(
            value["params"]["subscription"] == subscription,
            "unexpected follow subscription: {value}"
        );
        let event = value["params"]["result"].clone();
        ensure!(
            event["event"] != "stop",
            "follow subscription stopped; probe cannot continue"
        );
        Ok(event)
    }
}

fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field]
        .as_str()
        .with_context(|| format!("missing string field {field}: {value}"))
}

fn hashes(value: &Value, field: &str) -> Result<Vec<String>> {
    value[field]
        .as_array()
        .with_context(|| format!("missing array {field}: {value}"))?
        .iter()
        .map(|hash| {
            hash.as_str()
                .map(str::to_owned)
                .context("block hash is not a string")
        })
        .collect()
}

async fn unpin(
    rpc: &mut Rpc,
    subscription: &str,
    pinned: &mut HashSet<String>,
    candidates: HashSet<String>,
) -> Result<()> {
    let hashes: Vec<_> = candidates
        .into_iter()
        .filter(|hash| pinned.contains(hash))
        .collect();
    if !hashes.is_empty() {
        let result = rpc
            .call("chainHead_v1_unpin", json!([subscription, hashes]))
            .await?;
        ensure!(result.is_null(), "unexpected unpin result: {result}");
        for hash in hashes {
            pinned.remove(&hash);
        }
    }
    Ok(())
}

fn decode_number(value: &str) -> Result<u32> {
    let hex = value
        .strip_prefix("0x")
        .context("System.Number is not hex encoded")?;
    ensure!(
        hex.len() == 8 && hex.is_ascii(),
        "System.Number must be exactly four SCALE bytes: {value}"
    );
    let mut bytes = [0; 4];
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte =
            u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).context("invalid System.Number hex")?;
    }
    Ok(u32::from_le_bytes(bytes))
}

async fn storage(rpc: &mut Rpc, subscription: &str, genesis: &str, block: &str) -> Result<u32> {
    let started = rpc
        .call(
            "chainHead_v1_storage",
            json!([
                subscription, block, [{"key": NUMBER_KEY, "type": "value"}], null,
            ]),
        )
        .await?;
    ensure!(
        started["result"] == "started",
        "storage operation refused: {started}"
    );
    ensure!(
        started["discardedItems"].as_u64() == Some(0),
        "System.Number storage request discarded: {started}"
    );
    let operation = string(&started, "operationId")?;
    let mut deferred = VecDeque::new();
    let mut found: Option<(String, u32)> = None;
    loop {
        // Preserve other follow events while waiting for this operation. They
        // may describe the next finalized block, which needs its own read.
        let event = rpc.event(subscription).await?;
        let kind = string(&event, "event")?;
        if !kind.starts_with("operation") {
            deferred.push_back(json!({
                "jsonrpc": "2.0", "method": "chainHead_v1_followEvent",
                "params": {"subscription": subscription, "result": event},
            }));
            continue;
        }
        ensure!(
            string(&event, "operationId")? == operation,
            "unexpected storage operation: {event}"
        );
        match kind {
            "operationStorageItems" => {
                let items = event["items"]
                    .as_array()
                    .context("storage items is not an array")?;
                for item in items {
                    ensure!(item["key"] == NUMBER_KEY, "unexpected storage key: {item}");
                    let value = string(item, "value").context("System.Number is null or absent")?;
                    let number = decode_number(value)?;
                    if let Some((previous, _)) = &found {
                        ensure!(previous == value, "conflicting System.Number values");
                    } else {
                        found = Some((value.to_owned(), number));
                    }
                }
            }
            "operationStorageDone" => break,
            "operationInaccessible" | "operationError" => {
                bail!("storage operation failed: {event}")
            }
            _ => bail!("unexpected event for value-only storage operation: {event}"),
        }
    }
    deferred.append(&mut rpc.events);
    rpc.events = deferred;
    let (value, number) = found.context("System.Number storage is missing")?;
    emit(
        &json!({"event": "storage", "genesis_hash": genesis, "block_hash": block,
        "key": NUMBER_KEY, "value": value, "number": number}),
    )?;
    Ok(number)
}

async fn probe(socket: Socket, target: u32) -> Result<()> {
    let mut rpc = Rpc {
        socket,
        next_id: 1,
        events: VecDeque::new(),
    };
    let methods = rpc.call("rpc_methods", json!([])).await?;
    let methods = methods["methods"]
        .as_array()
        .context("rpc_methods has no methods array")?;
    for required in [
        "chainHead_v1_follow",
        "chainHead_v1_storage",
        "chainHead_v1_unpin",
        "chainHead_v1_unfollow",
        "chainSpec_v1_genesisHash",
    ] {
        ensure!(
            methods.iter().any(|method| method == required),
            "daemon does not support {required}"
        );
    }
    let genesis = rpc.call("chainSpec_v1_genesisHash", json!([])).await?;
    let genesis = genesis.as_str().context("genesis hash is not a string")?;
    // Header-only initialization can expose a checkpoint whose state peers pruned.
    // Wait until smoldot has obtained the finalized runtime before reading state.
    let subscription = rpc.call("chainHead_v1_follow", json!([true])).await?;
    let subscription = subscription
        .as_str()
        .context("follow subscription is not a string")?;
    let initialized = rpc.event(subscription).await?;
    ensure!(
        initialized["event"] == "initialized",
        "expected initialized event: {initialized}"
    );
    let initial = hashes(&initialized, "finalizedBlockHashes")?;
    let mut latest = initial
        .last()
        .context("initialized has no finalized blocks")?
        .clone();
    let mut pinned: HashSet<String> = initial.into_iter().collect();
    emit(&json!({"event": "initialized", "genesis_hash": genesis, "block_hash": latest}))?;
    let mut number = storage(&mut rpc, subscription, genesis, &latest).await?;
    let mut old = pinned.clone();
    old.remove(&latest);
    unpin(&mut rpc, subscription, &mut pinned, old).await?;
    let mut finalizations = 0;
    while finalizations < target {
        let event = rpc.event(subscription).await?;
        match string(&event, "event")? {
            "newBlock" => {
                pinned.insert(string(&event, "blockHash")?.to_owned());
            }
            "bestBlockChanged" => {}
            "finalized" => {
                let finalized = hashes(&event, "finalizedBlockHashes")?;
                let next = finalized
                    .last()
                    .context("finalized event has no finalized blocks")?;
                ensure!(
                    pinned.contains(next),
                    "finalized block was not announced: {next}"
                );
                let next_number = storage(&mut rpc, subscription, genesis, next).await?;
                ensure!(
                    next_number > number,
                    "finalized System.Number did not advance: {number} -> {next_number}"
                );
                let mut obsolete: HashSet<_> =
                    hashes(&event, "prunedBlockHashes")?.into_iter().collect();
                obsolete.extend(finalized.iter().cloned());
                obsolete.insert(latest);
                obsolete.remove(next);
                latest = next.clone();
                number = next_number;
                unpin(&mut rpc, subscription, &mut pinned, obsolete).await?;
                finalizations += 1;
                emit(&json!({"event": "finalized", "genesis_hash": genesis,
                    "block_hash": latest, "number": number, "finalizations": finalizations}))?;
            }
            _ => bail!("unexpected follow event: {event}"),
        }
    }
    let remaining = pinned.clone();
    unpin(&mut rpc, subscription, &mut pinned, remaining).await?;
    let result = rpc
        .call("chainHead_v1_unfollow", json!([subscription]))
        .await?;
    ensure!(result.is_null(), "unexpected unfollow result: {result}");
    rpc.socket
        .close(None)
        .await
        .context("closing probe WebSocket")?;
    emit(&json!({"event": "completed", "genesis_hash": genesis,
        "block_hash": latest, "number": number, "finalizations": finalizations}))?;
    Ok(())
}
