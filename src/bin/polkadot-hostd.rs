// SPDX-License-Identifier: GPL-3.0-or-later

use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use clap::Parser;
use polkadot_host::{
    default_socket_path,
    engine::{CheckpointClient, Engine},
    ipc::{self, BoundSocket},
    state::{StateStore, default_state_dir},
};
use tokio::{
    signal::unix::{SignalKind, signal},
    sync::{Semaphore, oneshot},
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
    /// Private persistent checkpoint directory (defaults to XDG_STATE_HOME/polkadot-host).
    #[arg(long, conflicts_with = "no_state")]
    state_dir: Option<PathBuf>,
    /// Run without reading or writing bootstrap state.
    #[arg(long)]
    no_state: bool,
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
    let specification = ipc::read_chain_spec(&args.chain_spec)?;
    let store = if args.no_state {
        None
    } else {
        let directory = args.state_dir.map(Ok).unwrap_or_else(default_state_dir)?;
        Some(Arc::new(StateStore::open(directory, &specification)?))
    };
    let database = store
        .as_ref()
        .map(|store| store.load())
        .transpose()?
        .flatten();
    if let Some(database) = &database {
        log::info!("host: checkpoint loaded bytes={}", database.len());
    } else if store.is_some() {
        log::info!("host: no checkpoint; starting from chain specification");
    }
    let path = args.socket.map(Ok).unwrap_or_else(default_socket_path)?;
    let bound = match inherited {
        Some(listener) => BoundSocket::from_inherited(path.clone(), listener)?,
        None => BoundSocket::bind(path.clone())?,
    };
    let (engine, chain, control) = Engine::new(
        specification,
        store.as_ref().map(|_| database.as_deref().unwrap_or("")),
    )?;
    // The engine consumes the bootstrap data while adding the anchor.
    drop(database);
    let checkpoint_worker = match (store, control) {
        (Some(store), Some(control)) => {
            let (shutdown, receiver) = oneshot::channel();
            Some((
                shutdown,
                tokio::spawn(checkpoints(store, control, receiver)),
            ))
        }
        _ => None,
    };
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
    if let Some((shutdown, mut worker)) = checkpoint_worker {
        let _ = shutdown.send(());
        match tokio::time::timeout(Duration::from_secs(12), &mut worker).await {
            Ok(saved) => saved.context("checkpoint worker failed")??,
            Err(error) => {
                worker.abort();
                return Err(error).context("shutdown checkpoint did not finish within 12 seconds");
            }
        }
    }
    log::info!("host: stopped");
    result
}

async fn checkpoints(
    store: Arc<StateStore>,
    mut control: CheckpointClient,
    mut shutdown: oneshot::Receiver<()>,
) -> Result<()> {
    let period = Duration::from_secs(60);
    let mut timer = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let stopping = tokio::select! {
            _ = &mut shutdown => true,
            _ = timer.tick() => false,
        };
        let saved = save_checkpoint(store.clone(), &mut control).await;
        if stopping {
            return saved.context("saving shutdown checkpoint");
        }
        if let Err(error) = saved {
            log::error!("host: periodic checkpoint failed: {error:#}");
        }
    }
}

async fn save_checkpoint(store: Arc<StateStore>, control: &mut CheckpointClient) -> Result<()> {
    let snapshot = tokio::time::timeout(Duration::from_secs(5), control.snapshot())
        .await
        .context("snapshot RPC exceeded five seconds")??;
    let height = snapshot.finalized_height;
    let bytes = snapshot.database.len();
    tokio::task::spawn_blocking(move || store.save(&snapshot.database))
        .await
        .context("checkpoint writer failed")??;
    log::info!("host: checkpoint saved finalized_height={height} bytes={bytes}");
    Ok(())
}
