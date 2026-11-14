// SPDX-License-Identifier: GPL-3.0-or-later

use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result, ensure};
use clap::Parser;
use polkadot_host::{
    default_socket_path,
    engine::Engine,
    ipc::{self, BoundSocket},
};
use tokio::{
    signal::unix::{SignalKind, signal},
    sync::Semaphore,
    task::JoinSet,
};

#[derive(Parser)]
#[command(
    version,
    about = "Experimental read-only light-client RPC over a private Unix socket"
)]
struct Args {
    /// Public relay-chain specification. The spike does not load parachains.
    #[arg(long)]
    chain_spec: PathBuf,
    /// Absolute socket path in a private, user-owned directory.
    #[arg(long)]
    socket: Option<PathBuf>,
    /// Require exactly one systemd-provided Unix listener; never bind a replacement.
    #[arg(long)]
    systemd_socket: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    // listenfd consumes environment variables. Do this before creating any threads.
    let inherited = if args.systemd_socket {
        ensure!(
            std::env::var("LISTEN_PID").ok().as_deref() == Some(&std::process::id().to_string()),
            "socket activation requires LISTEN_PID matching this process"
        );
        let mut descriptors = listenfd::ListenFd::from_env();
        ensure!(
            descriptors.len() == 1,
            "socket activation requires exactly one listener"
        );
        Some(
            descriptors
                .take_unix_listener(0)?
                .context("missing inherited Unix listener")?,
        )
    } else {
        None
    };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp(None)
        .init();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(args, inherited))
}

async fn run(args: Args, inherited: Option<std::os::unix::net::UnixListener>) -> Result<()> {
    let path = args.socket.map(Ok).unwrap_or_else(default_socket_path)?;
    let bound = match inherited {
        Some(listener) => BoundSocket::from_inherited(path.clone(), listener)?,
        None => BoundSocket::bind(path.clone())?,
    };
    let (engine, chain) = Engine::new(ipc::read_chain_spec(&args.chain_spec)?)?;
    let chain = Arc::new(chain);
    let permits = Arc::new(Semaphore::new(16));
    let mut tasks = JoinSet::new();
    let mut terminate = signal(SignalKind::terminate())?;
    log::info!("host: listening socket={}", path.display());

    let result: Result<()> = loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => { break signal.map_err(Into::into); }
            _ = terminate.recv() => break Ok(()),
            accepted = bound.listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => break Err(error.into()),
                };
                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    log::warn!("host: connection limit reached");
                    continue;
                };
                let engine = engine.clone();
                let chain = chain.clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    if let Err(error) = ipc::serve(stream, &chain, engine).await {
                        log::warn!("host: session ended: {error:#}");
                    }
                });
            }
            Some(completed) = tasks.join_next(), if !tasks.is_empty() => {
                if let Err(error) = completed { log::error!("host: session task failed: {error}"); }
            }
        }
    };
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    log::info!("host: stopped");
    result
}
