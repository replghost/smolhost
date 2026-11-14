// SPDX-License-Identifier: GPL-3.0-or-later

//! Fetch a CID through the local native Bitswap route; never overwrite output.
//! CID integrity does not establish chain inclusion, authorship, or safe contents.

use std::{path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};
use clap::Parser;
use smolhost::content::{
    MAX_FILE_BYTES,
    client::{FetchOptions, fetch},
};

#[derive(Parser)]
#[command(about = "Stream native Bulletin content to a verified, no-clobber local file")]
struct Args {
    /// Private smolhost Unix socket (defaults to $XDG_RUNTIME_DIR/smolhost/rpc.sock).
    #[arg(long)]
    socket: Option<PathBuf>,
    #[arg(long, default_value = "next-bulletin-paseo")]
    chain: String,
    /// CIDv1 base32 raw or UnixFS DAG-PB file root.
    #[arg(long)]
    cid: String,
    /// Destination must not already exist; partial files are private and removed.
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value_t = MAX_FILE_BYTES)]
    max_bytes: u64,
    #[arg(long, default_value_t = 180)]
    timeout_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let socket = args
        .socket
        .map(Ok)
        .unwrap_or_else(smolhost::default_socket_path)?;
    let options = FetchOptions {
        max_bytes: args.max_bytes,
        timeout: Duration::from_secs(args.timeout_secs),
    };
    let receipt = tokio::select! {
        biased;
        interrupted = tokio::signal::ctrl_c() => {
            interrupted.context("installing Ctrl-C handler")?;
            bail!("download cancelled; partial output removed");
        }
        receipt = fetch(&socket, &args.chain, &args.cid, &args.output, options) => receipt?,
    };
    println!("{}", serde_json::to_string(&receipt)?);
    Ok(())
}
