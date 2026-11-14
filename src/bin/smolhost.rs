// SPDX-License-Identifier: GPL-3.0-or-later

use std::{
    collections::{HashSet, VecDeque},
    io::{self, Write},
    path::PathBuf,
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use clap::{Parser, Subcommand};
use serde_json::{Value, json};
use smolhost::client::{Connection, RpcClient, connect_chain, discover};

const NUMBER_KEY: &str = "0x26aa394eea5630e07c48ae0c9558cef702a5c1b19ab7a04f536c519aca4983ac";

#[derive(Parser)]
#[command(about = "Read-only diagnostics for a local Smolhost daemon")]
struct Args {
    #[arg(long, global = true)]
    socket: Option<PathBuf>,
    #[arg(long)]
    chain: Option<String>,
    /// Total deadline, including connection and synchronization.
    #[arg(long, global = true, default_value_t = 240, value_parser = clap::value_parser!(u64).range(1..))]
    timeout_secs: u64,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Discover configured chains and static transport capabilities.
    Chains,
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
                bail!("command interrupted before completion")
            }
        }
    }
}

async fn run(args: &Args) -> Result<()> {
    match args.command {
        Command::Chains => ensure!(args.chain.is_none(), "--chain is not accepted with chains"),
        _ => ensure!(
            args.chain.is_some(),
            "--chain is required for probe and rpc"
        ),
    }
    let path = match &args.socket {
        Some(path) => path.clone(),
        None => smolhost::default_socket_path()?,
    };
    match &args.command {
        Command::Chains => emit(&serde_json::to_value(discover(&path).await?)?),
        Command::Rpc { json } => {
            raw_rpc(
                Connection::chain(&path, args.chain.as_deref().unwrap()).await?,
                json,
            )
            .await
        }
        Command::Probe { finalizations } => {
            let info = discover(&path).await?;
            let id = args.chain.as_deref().unwrap();
            let chain = info
                .chains
                .iter()
                .find(|chain| chain.id == id)
                .with_context(|| format!("host does not advertise chain {id}"))?;
            let client = connect_chain(&path, chain).await?;
            probe(client, &chain.genesis_hash, *finalizations).await
        }
    }
}

fn emit(value: &Value) -> Result<()> {
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, value)?;
    writeln!(stdout)?;
    stdout.flush()?;
    Ok(())
}

async fn raw_rpc(mut connection: Connection, payload: &str) -> Result<()> {
    let value: Value = serde_json::from_str(payload).context("invalid JSON RPC payload")?;
    connection.send_json(&value).await?;
    while let Some(value) = connection.receive_json().await? {
        emit(&value)?;
    }
    Ok(())
}

// Only the probe interprets chainHead events. Generic routing lives in RpcClient.
struct Probe {
    client: RpcClient,
    events: Events,
}

#[derive(Default)]
struct Events {
    values: VecDeque<(Value, usize)>,
    bytes: usize,
}

impl Events {
    fn push(&mut self, value: Value) -> Result<()> {
        struct Size(usize);
        impl Write for Size {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.0 += bytes.len();
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let mut size = Size(0);
        serde_json::to_writer(&mut size, &value)?;
        ensure!(
            self.values.len() < 256 && size.0 <= 8 * 1024 * 1024 - self.bytes,
            "probe deferred event backlog exceeds 256 envelopes or 8 MiB"
        );
        self.bytes += size.0;
        self.values.push_back((value, size.0));
        Ok(())
    }

    fn pop(&mut self) -> Option<Value> {
        self.values.pop_front().map(|(value, bytes)| {
            self.bytes -= bytes;
            value
        })
    }
}

impl Probe {
    async fn event(&mut self, subscription: &str) -> Result<Value> {
        let mut value = match self.events.pop() {
            Some(value) => value,
            None => self.client.next_notification().await?,
        };
        ensure!(
            value["method"] == "chainHead_v1_followEvent",
            "unexpected notification: {value}"
        );
        ensure!(
            value["params"]["subscription"] == subscription,
            "unexpected follow subscription: {value}"
        );
        let event = value["params"]["result"].take();
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
    rpc: &mut Probe,
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
            .client
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

async fn storage(rpc: &mut Probe, subscription: &str, genesis: &str, block: &str) -> Result<u32> {
    let started = rpc
        .client
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
    let mut deferred = Events::default();
    let mut found: Option<(String, u32)> = None;
    loop {
        // Preserve other follow events while waiting for this operation. They
        // may describe the next finalized block, which needs its own read.
        let event = rpc.event(subscription).await?;
        let kind = string(&event, "event")?;
        if !kind.starts_with("operation") {
            deferred.push(json!({
                "jsonrpc": "2.0", "method": "chainHead_v1_followEvent",
                "params": {"subscription": subscription, "result": event},
            }))?;
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
    ensure!(
        deferred.values.len() + rpc.events.values.len() <= 256
            && deferred.bytes <= 8 * 1024 * 1024 - rpc.events.bytes,
        "probe deferred event backlog exceeds 256 envelopes or 8 MiB"
    );
    deferred.bytes += rpc.events.bytes;
    deferred.values.append(&mut rpc.events.values);
    rpc.events = deferred;
    let (value, number) = found.context("System.Number storage is missing")?;
    emit(
        &json!({"event": "storage", "genesis_hash": genesis, "block_hash": block,
        "key": NUMBER_KEY, "value": value, "number": number}),
    )?;
    Ok(number)
}

async fn probe(client: RpcClient, genesis: &str, target: u32) -> Result<()> {
    let mut rpc = Probe {
        client,
        events: Events::default(),
    };
    let methods = rpc.client.call("rpc_methods", json!([])).await?;
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
    // Header-only initialization can expose a checkpoint whose state peers pruned.
    // Wait until smoldot has obtained the finalized runtime before reading state.
    let subscription = rpc
        .client
        .call("chainHead_v1_follow", json!([true]))
        .await?;
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
        .client
        .call("chainHead_v1_unfollow", json!([subscription]))
        .await?;
    ensure!(result.is_null(), "unexpected unfollow result: {result}");
    rpc.client.close().await?;
    emit(&json!({"event": "completed", "genesis_hash": genesis,
        "block_hash": latest, "number": number, "finalizations": finalizations}))?;
    Ok(())
}
