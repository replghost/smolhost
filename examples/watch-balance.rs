// SPDX-License-Identifier: GPL-3.0-or-later

//! Independent read-only native-PAS consumer of smolhost's public client adapter.
//! No CLI internals, signing, legacy state RPC, retries or remote RPC fallback.

mod balance;
mod shared;

use std::{
    io::{self, Write},
    path::PathBuf,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use balance::{AccountSchema, account_id, bytes};
use clap::Parser;
use serde_json::json;
use shared::{CLEANUP_TIMEOUT, CODE_KEY, Session, validate_hash};
use smolhost::client::{connect_chain, discover};

// Application trust anchor, deliberately independent of the daemon's catalog.
const GENESIS: &str = "0xd6eec26135305a8ad257a20d003357284c8aa03d0bdb2b357ab0a22371e11ef2";

#[derive(Parser)]
#[command(
    about = "Watch verified finalized native PAS balances on classic Asset Hub Paseo",
    long_about = "Independent read-only application using smolhost's public Unix-WebSocket client. Selects classic Asset Hub Paseo by a separately pinned genesis, not a chain name or SS58 prefix. Reads System.Account using verified runtime metadata (V14–V16), with exact integer base-unit JSONL output. Watches native PAS only, not assets or foreign tokens. Runtime code changes fail closed; restart explicitly after reviewing the upgrade. No retries, signing, keys or remote RPC fallback."
)]
struct Args {
    /// Daemon Unix socket (default: $XDG_RUNTIME_DIR/smolhost/rpc.sock)
    #[arg(long)]
    socket: Option<PathBuf>,
    /// SS58 AccountId32 (any nonreserved network prefix), or 0x + exactly 64 hex digits
    #[arg(long)]
    address: String,
    /// Additional finalized blocks to read after the initial finalized block (0 reads initial only)
    #[arg(long, default_value_t = 5)]
    finalizations: u32,
    /// Overall discovery/sync/read deadline; cleanup may take up to 2 additional seconds
    #[arg(long, default_value_t = 180, value_parser = clap::value_parser!(u64).range(1..))]
    timeout_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let account = account_id(&args.address).context("invalid --address")?;
    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(args.timeout_secs))
        .context("--timeout-secs is too large for a monotonic deadline")?;
    let mut session = None;
    let outcome = tokio::select! {
        result = watch(&args, &account, &mut session) => result,
        _ = tokio::time::sleep_until(deadline) => Err(anyhow::anyhow!("overall deadline exceeded after {} seconds", args.timeout_secs)),
        signal = tokio::signal::ctrl_c() => {
            signal.context("could not listen for Ctrl-C")
                .and(Err(anyhow::anyhow!("interrupted by Ctrl-C")))
        }
    };
    if let Some(mut session) = session {
        // Cancellation can poison an in-flight RpcClient call. Cleanup attempts
        // protocol shutdown where still usable, and always attempts transport close;
        // dropping the session releases all server-side pins/operations even on timeout.
        let cleanup = tokio::time::timeout(CLEANUP_TIMEOUT, session.cleanup())
            .await
            .context("session cleanup exceeded 2 seconds")
            .and_then(|result| result);
        if outcome.is_ok() {
            cleanup?;
        } else if let Err(error) = cleanup {
            eprintln!("balance watcher cleanup: {error:#}");
        }
    }
    outcome
}

async fn watch(args: &Args, account: &[u8; 32], holder: &mut Option<Session>) -> Result<()> {
    let socket = args
        .socket
        .clone()
        .map(Ok)
        .unwrap_or_else(smolhost::default_socket_path)?;
    let catalog = discover(&socket).await?;
    let mut matches = catalog
        .chains
        .iter()
        .filter(|chain| chain.genesis_hash == GENESIS);
    let chain = matches
        .next()
        .context("daemon has no classic Asset Hub Paseo route with the pinned genesis")?;
    ensure!(
        matches.next().is_none(),
        "ambiguous routes for pinned Asset Hub Paseo genesis"
    );
    let client = connect_chain(&socket, chain).await?;
    *holder = Some(Session::new(client));
    let session = holder.as_mut().unwrap();
    session.initialize().await?;

    let mut schema: Option<AccountSchema> = None;
    let mut runtime_code_hash: Option<String> = None;
    let account_hex = format!("0x{}", hex::encode(account));
    for index in 0..=args.finalizations {
        let block = session.next_finalized().await?;
        // :code is queried as a hash, not downloaded. A code change is an explicit
        // upgrade boundary: never decode new-runtime storage using cached metadata.
        let code_hash = session
            .storage(&block, CODE_KEY, "hash")
            .await?
            .context("finalized runtime :code is absent")?;
        validate_hash(&code_hash)?;
        if let Some(previous) = &runtime_code_hash {
            ensure!(
                previous == &code_hash,
                "runtime upgrade at {block}: :code hash changed from {previous} to {code_hash}; refusing stale metadata, restart explicitly"
            );
        } else {
            let output = session.runtime_metadata(&block).await?;
            schema = Some(
                AccountSchema::from_runtime_output(&output, account)
                    .context("incompatible verified runtime metadata")?,
            );
            runtime_code_hash = Some(code_hash.clone());
        }
        let schema = schema.as_ref().unwrap();
        let stored = session
            .storage(&block, &schema.storage_key, "value")
            .await?;
        let encoded = stored.as_deref().map(bytes).transpose()?;
        let balances = schema.decode(encoded.as_deref())?;
        let record = json!({
            "event": "balance", "genesis_hash": GENESIS, "block_hash": block,
            "account_id": account_hex, "exists": encoded.is_some(),
            "asset": "PAS", "unit": "base", "balances": balances,
            "runtime_code_hash": code_hash, "metadata_version": schema.metadata_version,
            "finalization_index": index,
        });
        let mut stdout = io::stdout().lock();
        serde_json::to_writer(&mut stdout, &record)?;
        writeln!(stdout)?;
        stdout.flush()?;
        drop(stdout);
        session.release(block).await?;
    }
    Ok(())
}
