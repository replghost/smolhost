// SPDX-License-Identifier: GPL-3.0-or-later

//! Read-only Paseo Next v2 app profile, exclusively through the public Unix client.
//! Asset Hub: native public-account balances. Bulletin: data retention policy in
//! blocks, not blob retrieval or proof of a particular blob's availability.
//! Individuality/People: old membership-ring-root grace period in seconds, not
//! identity enumeration or a full personhood guarantee. No gossip, signing or tx.
//! Each chain is an independent finalized snapshot, not an atomic cross-chain view.
//! Metadata and values are verified at the SAME pinned block; there is no cached
//! schema across runtime upgrades, no public-RPC fallback and no automatic retry.
//!
//! Independent genesis anchors: paritytech/product-sdk revision
//! 964e6a3eee628c77bb294c0f7b62b210c841e9eb, product-sdk/packages/descriptors/chains/
//! paseo-{asset-hub,bulletin,individuality}/.papi/polkadot-api.json.
//! Domain schema evidence is linked in app_profile/mod.rs.

mod app_profile;
mod balance;
mod shared;

use std::{
    io::{self, Write},
    path::PathBuf,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use app_profile::ScalarRead;
use balance::{AccountSchema, account_id, bytes};
use clap::Parser;
use serde_json::{Value, json};
use shared::{CLEANUP_TIMEOUT, CODE_KEY, Session, validate_hash};
use smolhost::client::{connect_chain, discover};

#[derive(Clone, Copy)]
struct Role {
    name: &'static str,
    genesis: &'static str,
}

const ROLES: [Role; 3] = [
    Role {
        name: "asset-hub",
        genesis: "0x4349b00e54897e21196fd331015fc5be0f14e118beb0375ed2bb1793737bb57a",
    },
    Role {
        name: "bulletin",
        genesis: "0x8cfe6717dc4becfda2e13c488a1e2061ff2dfee96e7d031157f72d36716c0a22",
    },
    Role {
        name: "individuality",
        genesis: "0x4a2b5b737de1da59e209b0000a876ec2fa20035dc34fd292a848da32d255ad48",
    },
];

#[derive(Parser)]
#[command(
    about = "Read verified finalized app state on all three pinned Paseo Next v2 chains",
    long_about = "Read all three independently genesis-pinned Paseo Next v2 roles through smolhost's public Unix client: Asset Hub System.Account native balances; Bulletin TransactionStorage.RetentionPeriod storage (blocks); Individuality/People Members.OldRootRetentionDuration metadata constant (seconds). JSONL distinguishes stored/default values from verified runtime metadata constants; integers are exact decimal strings. Each chain has its own finalized block, runtime code hash and metadata version (V14–V16). This is not classic Paseo Asset Hub or Products Devnet. No blob retrieval, gossip, identity enumeration, full personhood guarantee, signing, transactions, retries or remote-RPC fallback. Missing roles, ambiguous genesis routes or unsupported schemas fail closed; a failure after earlier records exits nonzero."
)]
struct Args {
    /// Daemon Unix socket (default: $XDG_RUNTIME_DIR/smolhost/rpc.sock)
    #[arg(long)]
    socket: Option<PathBuf>,
    /// Public SS58 AccountId32 or 0x + 64 hex digits; default is public development Alice
    #[arg(
        long,
        default_value = "0xd43593c715fdd31c61141abd04a99fd6822c8558854ccde39a5684e7a56da27d"
    )]
    address: String,
    /// Overall discovery/sync/read deadline; cancellation cleanup may add 2 seconds
    #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..))]
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
        result = read_profile(&args, &account, &mut session) => result,
        _ = tokio::time::sleep_until(deadline) => Err(anyhow::anyhow!("overall deadline exceeded after {} seconds", args.timeout_secs)),
        signal = tokio::signal::ctrl_c() => {
            signal.context("could not listen for Ctrl-C")
                .and(Err(anyhow::anyhow!("interrupted by Ctrl-C")))
        }
    };
    let cleanup = cleanup(&mut session).await;
    if outcome.is_ok() {
        cleanup?;
    } else if let Err(error) = cleanup {
        eprintln!("app profile cleanup: {error:#}");
    }
    outcome
}

async fn cleanup(holder: &mut Option<Session>) -> Result<()> {
    if let Some(mut session) = holder.take() {
        // A cancelled call may poison RpcClient. Always attempt close; dropping
        // the transport releases remaining server-side operations and pins.
        tokio::time::timeout(CLEANUP_TIMEOUT, session.cleanup())
            .await
            .context("session cleanup exceeded 2 seconds")??;
    }
    Ok(())
}

async fn read_profile(args: &Args, account: &[u8; 32], holder: &mut Option<Session>) -> Result<()> {
    let socket = args
        .socket
        .clone()
        .map(Ok)
        .unwrap_or_else(smolhost::default_socket_path)?;
    let catalog = discover(&socket).await?;
    // Validate the entire profile before reading or emitting any role. A catalog
    // name, endpoint, SS58 prefix or daemon-selected network is never a trust anchor.
    let [asset_hub, bulletin, individuality] = ROLES.map(|role| -> Result<_> {
        let mut matches = catalog
            .chains
            .iter()
            .filter(|chain| chain.genesis_hash == role.genesis);
        let chain = matches.next().with_context(|| {
            format!(
                "no {} route with pinned genesis {}",
                role.name, role.genesis
            )
        })?;
        ensure!(
            matches.next().is_none(),
            "ambiguous {} genesis routes",
            role.name
        );
        Ok((role, chain))
    });
    for (role, chain) in [asset_hub?, bulletin?, individuality?] {
        *holder = Some(Session::new(connect_chain(&socket, chain).await?));
        let session = holder.as_mut().unwrap();
        let record = read_role(role, account, session)
            .await
            .with_context(|| format!("{} verified read failed", role.name))?;
        cleanup(holder).await?;
        let mut stdout = io::stdout().lock();
        serde_json::to_writer(&mut stdout, &record)?;
        writeln!(stdout)?;
        stdout.flush()?;
    }
    Ok(())
}

async fn read_role(role: Role, account: &[u8; 32], session: &mut Session) -> Result<Value> {
    session.initialize().await?;
    let block = session.next_finalized().await?;
    let code_hash = session
        .storage(&block, CODE_KEY, "hash")
        .await?
        .context("finalized runtime :code is absent")?;
    validate_hash(&code_hash)?;
    let metadata = session.runtime_metadata(&block).await?;
    let mut record = json!({
        "event": "app_profile_read", "role": role.name, "genesis_hash": role.genesis,
        "block_hash": block, "finality": "finalized", "runtime_code_hash": code_hash,
    });
    if role.name == "asset-hub" {
        let schema = AccountSchema::from_runtime_output(&metadata, account)?;
        let stored = session
            .storage(&block, &schema.storage_key, "value")
            .await?;
        let encoded = stored.as_deref().map(bytes).transpose()?;
        record["source_kind"] = json!("storage");
        record["pallet"] = json!("System");
        record["item"] = json!("Account");
        record["storage_key"] = json!(schema.storage_key);
        record["metadata_version"] = json!(schema.metadata_version);
        record["account_id"] = json!(format!("0x{}", hex::encode(account)));
        record["presence"] = json!(if encoded.is_some() {
            "present"
        } else {
            "default"
        });
        record["asset"] = json!("PAS");
        record["unit"] = json!("base");
        record["result"] = json!({ "type": "native_balances_u128", "balances": schema.decode(encoded.as_deref())? });
    } else {
        let (schema, pallet, item, unit) = if role.name == "bulletin" {
            (
                ScalarRead::retention(&metadata)?,
                "TransactionStorage",
                "RetentionPeriod",
                "blocks",
            )
        } else {
            (
                ScalarRead::old_root_retention(&metadata)?,
                "Members",
                "OldRootRetentionDuration",
                "seconds",
            )
        };
        let encoded = if let Some(key) = schema.storage_key() {
            record["source_kind"] = json!("storage");
            record["storage_key"] = json!(key);
            session
                .storage(&block, key, "value")
                .await?
                .as_deref()
                .map(bytes)
                .transpose()?
        } else {
            record["source_kind"] = json!("runtime_metadata_constant");
            None
        };
        let mut decoded = schema.decode(encoded.as_deref())?;
        record["pallet"] = json!(pallet);
        record["item"] = json!(item);
        record["metadata_version"] = json!(schema.metadata_version);
        record["unit"] = json!(unit);
        record["presence"] = decoded["presence"].take();
        record["result"] = decoded["result"].take();
    }
    session.release(block).await?;
    Ok(record)
}
