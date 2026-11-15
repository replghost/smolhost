// SPDX-License-Identifier: GPL-3.0-or-later

//! Application-level recovery of verified state, not replay of historical events.
//! Each replacement connection is independently discovered and genesis-checked.

mod recovery_output;
mod shared;

use std::{
    future::Future,
    io,
    path::{Path, PathBuf},
    process::ExitCode,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use clap::Parser;
use recovery_output::{Output, report_stderr};
use serde_json::json;
use shared::{CLEANUP_TIMEOUT, Interruption, Session, validate_hash};
use smolhost::client::{connect_chain, discover};
use tokio::{
    signal::unix::{Signal, SignalKind, signal},
    time::Instant,
};
use tokio_tungstenite::tungstenite::{Error as WebSocketError, error::ProtocolError};

const GENESIS: &str = "0x8cfe6717dc4becfda2e13c488a1e2061ff2dfee96e7d031157f72d36716c0a22";
const NUMBER_KEY: &str = "0x26aa394eea5630e07c48ae0c9558cef702a5c1b19ab7a04f536c519aca4983ac";

#[derive(Parser)]
#[command(
    about = "Watch verified Paseo Next Bulletin state with bounded, visible application recovery"
)]
struct Args {
    /// Daemon Unix socket (default: $XDG_RUNTIME_DIR/smolhost/rpc.sock)
    #[arg(long)]
    socket: Option<PathBuf>,
    /// Number of emitted snapshots, including unchanged catch-up snapshots
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u32).range(1..))]
    snapshots: u32,
    /// Total deadline; finishing allows 2s plus a 100ms best-effort diagnostic
    #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..))]
    timeout_secs: u64,
    /// Deadline for each discovery, connection, initialization, or finalized read
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..))]
    step_timeout_secs: u64,
    /// Lifetime recovery budget; never reset by a successful subscription or read
    #[arg(long, default_value_t = 8)]
    max_recoveries: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetryReason {
    OperationInaccessible,
    FollowStopped,
    TransportDisconnected,
    TransportTimeout,
    StepTimeout,
}

impl RetryReason {
    fn code(self) -> &'static str {
        match self {
            Self::OperationInaccessible => "operation_inaccessible",
            Self::FollowStopped => "follow_stopped",
            Self::TransportDisconnected => "transport_disconnected",
            Self::TransportTimeout => "transport_timeout",
            Self::StepTimeout => "step_timeout",
        }
    }
}

#[derive(Debug)]
struct StepTimeout;

impl std::fmt::Display for StepTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("session step deadline exceeded")
    }
}

impl std::error::Error for StepTimeout {}

fn io_reason(kind: io::ErrorKind) -> Option<RetryReason> {
    use io::ErrorKind::*;
    match kind {
        TimedOut => Some(RetryReason::TransportTimeout),
        NotFound | ConnectionRefused | ConnectionReset | ConnectionAborted | NotConnected
        | BrokenPipe | UnexpectedEof => Some(RetryReason::TransportDisconnected),
        _ => None,
    }
}

// Call only on session/transport work. In particular a BrokenPipe on stdout is
// terminal, not a reason to reconnect. No error-message matching is permitted.
fn classify(error: &anyhow::Error) -> Option<RetryReason> {
    for cause in error.chain() {
        if let Some(interruption) = cause.downcast_ref::<Interruption>() {
            return Some(match interruption {
                Interruption::OperationInaccessible => RetryReason::OperationInaccessible,
                Interruption::FollowStopped => RetryReason::FollowStopped,
            });
        }
        if cause.is::<StepTimeout>() {
            return Some(RetryReason::StepTimeout);
        }
        if let Some(error) = cause.downcast_ref::<WebSocketError>() {
            return match error {
                WebSocketError::ConnectionClosed
                | WebSocketError::Protocol(
                    ProtocolError::ResetWithoutClosingHandshake
                    | ProtocolError::HandshakeIncomplete,
                ) => Some(RetryReason::TransportDisconnected),
                WebSocketError::Io(error) => io_reason(error.kind()),
                _ => None,
            };
        }
        if let Some(error) = cause.downcast_ref::<io::Error>() {
            return io_reason(error.kind());
        }
        if cause.is::<tokio::time::error::Elapsed>() {
            return Some(RetryReason::TransportTimeout);
        }
    }
    None
}

async fn step<T>(duration: Duration, work: impl Future<Output = Result<T>>) -> Result<T> {
    let deadline = Instant::now()
        .checked_add(duration)
        .context("--step-timeout-secs is too large for a monotonic deadline")?;
    tokio::time::timeout_at(deadline, work)
        .await
        .map_err(|_| anyhow::Error::new(StepTimeout))?
}

#[derive(Default)]
struct Budget {
    used: u32,
}

impl Budget {
    fn attempt(&self) -> u64 {
        u64::from(self.used) + 1
    }

    fn reserve(&mut self, maximum: u32) -> Result<Duration> {
        ensure!(
            self.used < maximum,
            "lifetime recovery budget exhausted ({maximum})"
        );
        self.used += 1;
        Ok(Duration::from_millis(1000_u64 << (self.used - 1).min(4)))
    }
}

fn number(value: Option<&str>) -> Result<u32> {
    let encoded = value.context("verified System.Number is absent")?;
    let encoded = encoded
        .strip_prefix("0x")
        .context("System.Number lacks 0x prefix")?;
    let mut bytes = [0; 4];
    hex::decode_to_slice(encoded, &mut bytes)
        .context("System.Number must be exactly four little-endian SCALE bytes")?;
    Ok(u32::from_le_bytes(bytes))
}

#[derive(Default)]
struct Progress {
    snapshots: u32,
    // Keep only the last emitted identity, not an ever-growing history/index.
    last: Option<(u32, [u8; 32])>,
}

impl Progress {
    fn last_number(&self) -> Option<u32> {
        self.last.map(|(number, _)| number)
    }

    // Returns false for a behind head, leaving the replacement in catch-up mode.
    async fn observe(
        &mut self,
        mut block: String,
        number: u32,
        first: bool,
        attempt: u64,
        output: &mut Output,
    ) -> Result<bool> {
        validate_hash(&block)?;
        block.make_ascii_lowercase();
        let mut hash = [0; 32];
        hex::decode_to_slice(&block[2..], &mut hash)?;
        if let Some((last_number, last_hash)) = self.last {
            ensure!(
                hash != last_hash || number == last_number,
                "same block hash has conflicting block numbers"
            );
            ensure!(
                number != last_number || hash == last_hash,
                "equal block number has conflicting finalized hashes"
            );
            if number < last_number {
                output
                    .emit(json!({"event": "behind", "number": number, "last_number": last_number}))
                    .await?;
                return Ok(false);
            }
        }
        let kind = if self.last.is_none() {
            "initial"
        } else if first {
            "catch_up"
        } else {
            "update"
        };
        output
            .emit(json!({
                "event": "snapshot", "kind": kind, "attempt": attempt,
                "block_hash": block, "number": number, "previous_number": self.last_number(),
                "changed": self.last != Some((number, hash)), "historical_events_replayed": false,
            }))
            .await?;
        self.last = Some((number, hash));
        self.snapshots += 1;
        Ok(true)
    }
}

async fn open(socket: &Path, duration: Duration, holder: &mut Option<Session>) -> Result<()> {
    let catalog = step(duration, discover(socket)).await?;
    let mut matches = catalog
        .chains
        .iter()
        .filter(|chain| chain.genesis_hash == GENESIS);
    let chain = matches
        .next()
        .context("daemon has no route with the pinned Bulletin genesis")?;
    ensure!(
        matches.next().is_none(),
        "ambiguous routes for pinned Bulletin genesis"
    );
    let client = step(duration, connect_chain(socket, chain)).await?;
    // Install before initialize's first await so cancellation retains a cleanup target.
    *holder = Some(Session::new(client));
    step(duration, holder.as_mut().unwrap().initialize()).await
}

async fn head(session: &mut Session) -> Result<(String, u32)> {
    let block = session.next_finalized().await?;
    let stored = session.storage(&block, NUMBER_KEY, "value").await?;
    let number = number(stored.as_deref())?;
    // Release is inside the same bounded step, including heads skipped as behind.
    session.release(block.clone()).await?;
    Ok((block, number))
}

async fn dispose(holder: &mut Option<Session>) -> Result<()> {
    let result = if let Some(session) = holder.as_mut() {
        tokio::time::timeout(CLEANUP_TIMEOUT, session.cleanup())
            .await
            .context("session cleanup exceeded two seconds")
            .and_then(|result| result)
    } else {
        Ok(())
    };
    // A failed or cancelled RPC is never reused, even when protocol cleanup fails.
    holder.take();
    result
}

async fn watch(
    args: &Args,
    socket: &Path,
    holder: &mut Option<Session>,
    progress: &mut Progress,
    budget: &mut Budget,
    output: &mut Output,
) -> Result<()> {
    let duration = Duration::from_secs(args.step_timeout_secs);
    loop {
        let mut result = open(socket, duration, holder).await;
        if result.is_ok() {
            output.emit(json!({"event": "subscribed", "attempt": budget.attempt(), "genesis_hash": GENESIS})).await?;
            let mut first = true;
            while progress.snapshots < args.snapshots {
                match step(duration, head(holder.as_mut().unwrap())).await {
                    Ok((block, number)) => {
                        if progress
                            .observe(block, number, first, budget.attempt(), output)
                            .await?
                        {
                            first = false;
                        }
                    }
                    Err(error) => {
                        result = Err(error);
                        break;
                    }
                }
            }
        }
        let error = match result {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
        let Some(reason) = classify(&error) else {
            return Err(error);
        };
        if let Err(error) = dispose(holder).await {
            output
                .emit(json!({"event": "cleanup_warning", "message": format!("{error:#}")}))
                .await?;
        }
        let delay = budget
            .reserve(args.max_recoveries)
            .with_context(|| format!("last interruption: {error:#}"))?;
        output.emit(json!({
            "event": "retry", "recovery": budget.used, "next_attempt": budget.attempt(),
            "reason": reason.code(), "delay_ms": delay.as_millis(), "last_number": progress.last_number(),
        })).await?;
        tokio::time::sleep(delay).await;
    }
}

struct Signals {
    interrupt: Signal,
    terminate: Signal,
}

impl Signals {
    fn new() -> Result<Self> {
        Ok(Self {
            interrupt: signal(SignalKind::interrupt()).context("installing SIGINT handler")?,
            terminate: signal(SignalKind::terminate()).context("installing SIGTERM handler")?,
        })
    }

    async fn next(&mut self) -> Result<&'static str> {
        tokio::select! {
            value = self.interrupt.recv() => value.context("SIGINT stream closed").map(|_| "sigint"),
            value = self.terminate.recv() => value.context("SIGTERM stream closed").map(|_| "sigterm"),
        }
    }
}

async fn run(args: Args, output: &mut Output) -> Result<()> {
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(args.timeout_secs))
        .context("--timeout-secs is too large for a monotonic deadline")?;
    let socket = args
        .socket
        .clone()
        .map(Ok)
        .unwrap_or_else(smolhost::default_socket_path)?;
    let mut signals = Signals::new()?;
    let mut holder = None;
    let mut progress = Progress::default();
    let mut budget = Budget::default();
    let mut outcome = tokio::select! {
        biased;
        reason = signals.next() => reason,
        _ = tokio::time::sleep_until(deadline) => Err(anyhow::anyhow!("overall deadline exceeded")),
        result = watch(&args, &socket, &mut holder, &mut progress, &mut budget, output) => result.map(|()| "completed"),
    };
    let finish_deadline = if matches!(outcome, Ok("completed")) {
        deadline.min(Instant::now() + CLEANUP_TIMEOUT)
    } else {
        Instant::now() + CLEANUP_TIMEOUT
    };
    // One grace period covers protocol disposal AND final output. An interrupted
    // output stream is never reused: its last record may only be partly written.
    let finishing =
        async {
            if let Err(error) = dispose(&mut holder).await {
                let warning = format!("{error:#}");
                if matches!(outcome, Ok("completed")) {
                    outcome = Err(error.context("completed reads but cleanup failed"));
                }
                if output.usable() {
                    output
                        .emit(json!({"event": "cleanup_warning", "message": warning}))
                        .await?;
                }
            }
            ensure!(
                output.usable(),
                "terminal JSONL unavailable after interrupted output"
            );
            output.emit(json!({
            "event": "stopped", "reason": outcome.as_ref().map_or("failed", |reason| *reason),
            "snapshots": progress.snapshots, "recoveries": budget.used,
        })).await
        };
    let final_status = tokio::select! {
        biased;
        reason = signals.next() => reason.map(Some),
        _ = tokio::time::sleep_until(finish_deadline) => {
            Err(anyhow::anyhow!("finishing deadline exceeded; dropping session and output"))
        }
        result = finishing => result.map(|()| None),
    };
    holder.take();
    match final_status {
        Ok(None) => {}
        Ok(Some(reason)) => {
            if outcome.is_ok() {
                outcome = Ok(reason);
                let _ = report_stderr(&format!("stopped: {reason}; final JSONL interrupted")).await;
            } else {
                outcome = outcome.context(format!("finishing interrupted by {reason}"));
            }
        }
        Err(error) => {
            if matches!(outcome, Ok("completed")) {
                outcome = Err(error);
            } else if let Ok(reason) = outcome.as_ref() {
                let _ = report_stderr(&format!("stopped: {reason}; {error:#}")).await;
            } else {
                outcome = outcome.context(format!("final status unavailable: {error:#}"));
            }
        }
    }
    outcome.map(|_| ())
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    let result = match Output::stdout() {
        Ok(mut output) => run(args, &mut output).await,
        Err(error) => Err(error),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            // Rust's Result termination printer would itself block on stderr.
            let _ = report_stderr(&format!("Error: {error:#}")).await;
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::{
        io::Write,
        sync::{Arc, Mutex},
    };

    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl Write for Capture {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn captured_output() -> (Output, Capture) {
        let captured = Capture::default();
        (Output::from_writer(captured.clone()).unwrap(), captured)
    }

    fn hash(byte: u8) -> String {
        format!("0x{}", hex::encode([byte; 32]))
    }

    fn records(output: &Capture) -> Vec<Value> {
        output
            .0
            .lock()
            .unwrap()
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect()
    }

    #[test]
    fn classification_is_typed_and_strict() {
        for (error, expected) in [
            (
                anyhow::Error::new(Interruption::OperationInaccessible),
                Some(RetryReason::OperationInaccessible),
            ),
            (
                anyhow::Error::new(Interruption::FollowStopped),
                Some(RetryReason::FollowStopped),
            ),
            (
                anyhow::Error::new(StepTimeout),
                Some(RetryReason::StepTimeout),
            ),
            (
                anyhow::Error::new(WebSocketError::ConnectionClosed),
                Some(RetryReason::TransportDisconnected),
            ),
            (
                anyhow::Error::new(WebSocketError::Protocol(
                    ProtocolError::ResetWithoutClosingHandshake,
                )),
                Some(RetryReason::TransportDisconnected),
            ),
            (
                anyhow::Error::new(WebSocketError::Protocol(ProtocolError::HandshakeIncomplete)),
                Some(RetryReason::TransportDisconnected),
            ),
            (
                anyhow::Error::new(WebSocketError::Protocol(ProtocolError::SendAfterClosing)),
                None,
            ),
            (anyhow::Error::new(WebSocketError::AlreadyClosed), None),
            (
                anyhow::anyhow!("operationInaccessible: Connection reset by peer"),
                None,
            ),
            (serde_json::from_str::<Value>("{").unwrap_err().into(), None),
        ] {
            assert_eq!(classify(&error.context("session step")), expected);
        }
        for kind in [
            io::ErrorKind::NotFound,
            io::ErrorKind::ConnectionRefused,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::NotConnected,
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::UnexpectedEof,
            io::ErrorKind::TimedOut,
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::InvalidData,
            io::ErrorKind::Other,
        ] {
            let expected = match kind {
                io::ErrorKind::PermissionDenied
                | io::ErrorKind::InvalidData
                | io::ErrorKind::Other => None,
                io::ErrorKind::TimedOut => Some(RetryReason::TransportTimeout),
                _ => Some(RetryReason::TransportDisconnected),
            };
            assert_eq!(classify(&io::Error::from(kind).into()), expected);
            assert_eq!(
                classify(&WebSocketError::Io(io::Error::from(kind)).into()),
                expected
            );
        }
    }

    #[tokio::test]
    async fn transport_and_application_deadlines_have_distinct_reasons() {
        let elapsed = tokio::time::timeout(Duration::ZERO, std::future::pending::<()>())
            .await
            .unwrap_err();
        assert_eq!(
            classify(&elapsed.into()),
            Some(RetryReason::TransportTimeout)
        );
        let error = step(Duration::ZERO, std::future::pending::<Result<()>>())
            .await
            .unwrap_err();
        assert_eq!(classify(&error), Some(RetryReason::StepTimeout));
    }

    #[tokio::test]
    async fn recovery_budget_is_lifetime_and_delays_are_capped() {
        let mut budget = Budget::default();
        assert!(budget.reserve(0).is_err());
        assert_eq!(budget.attempt(), 1);
        let mut progress = Progress::default();
        let mut output = Output::from_writer(io::sink()).unwrap();
        for (index, millis) in [1000, 2000, 4000, 8000, 16000, 16000, 16000, 16000]
            .into_iter()
            .enumerate()
        {
            assert_eq!(budget.reserve(8).unwrap(), Duration::from_millis(millis));
            // Even a successful replacement read cannot replenish this budget.
            progress
                .observe(
                    hash(index as u8),
                    index as u32,
                    true,
                    budget.attempt(),
                    &mut output,
                )
                .await
                .unwrap();
        }
        assert!(budget.reserve(8).is_err());
        assert_eq!(budget.used, 8);
        assert_eq!(budget.attempt(), 9);
    }

    #[tokio::test]
    async fn catch_up_skips_old_heads_and_reports_unchanged_snapshot() {
        let mut progress = Progress::default();
        let (mut output, captured) = captured_output();
        assert!(
            progress
                .observe(hash(10), 10, true, 3, &mut output)
                .await
                .unwrap()
        );
        assert!(
            !progress
                .observe(hash(8), 8, true, 4, &mut output)
                .await
                .unwrap()
        );
        assert!(
            !progress
                .observe(hash(9), 9, true, 4, &mut output)
                .await
                .unwrap()
        );
        assert_eq!(progress.last_number(), Some(10));
        assert_eq!(progress.snapshots, 1);
        assert!(
            progress
                .observe(
                    hash(10).to_uppercase().replacen("0X", "0x", 1),
                    10,
                    true,
                    4,
                    &mut output
                )
                .await
                .unwrap()
        );
        assert!(
            progress
                .observe(hash(11), 11, false, 4, &mut output)
                .await
                .unwrap()
        );
        let records = records(&captured);
        assert_eq!(records[0]["kind"], "initial");
        assert!(records[0]["previous_number"].is_null());
        assert_eq!(
            records[1],
            json!({"event":"behind", "number":8, "last_number":10})
        );
        assert_eq!(records[3]["kind"], "catch_up");
        assert_eq!(records[3]["changed"], false);
        assert_eq!(records[3]["previous_number"], 10);
        assert_eq!(records[4]["kind"], "update");
        assert_eq!(records[4]["changed"], true);
        assert_eq!(progress.snapshots, 3);
        assert!(
            records
                .iter()
                .filter(|record| record["event"] == "snapshot")
                .all(|record| record["historical_events_replayed"] == false)
        );
    }

    #[tokio::test]
    async fn catch_up_can_jump_forward_but_conflicting_identities_are_fatal() {
        let mut progress = Progress::default();
        let (mut output, captured) = captured_output();
        progress
            .observe(hash(1), 10, true, 1, &mut output)
            .await
            .unwrap();
        progress
            .observe(hash(2), 20, true, 2, &mut output)
            .await
            .unwrap();
        let before = captured.0.lock().unwrap().len();
        for (block, number) in [(hash(3), 20), (hash(2), 21), (hash(2), 19)] {
            let error = progress
                .observe(block, number, true, 3, &mut output)
                .await
                .unwrap_err();
            assert_eq!(classify(&error), None);
        }
        assert_eq!(captured.0.lock().unwrap().len(), before);
        assert_eq!(progress.last_number(), Some(20));
        let records = records(&captured);
        assert_eq!(records[1]["kind"], "catch_up");
        assert_eq!(records[1]["previous_number"], 10);
        assert_eq!(records[1]["changed"], true);
    }

    #[test]
    fn number_decode_requires_exact_verified_scale_value() {
        assert_eq!(number(Some("0x78563412")).unwrap(), 0x12345678);
        assert_eq!(number(Some("0xFFFFFFFF")).unwrap(), u32::MAX);
        for value in [
            None,
            Some("0x"),
            Some("00000000"),
            Some("0x000000"),
            Some("0x0000000000"),
            Some("0x0000000g"),
        ] {
            let error = number(value).unwrap_err();
            assert_eq!(classify(&error), None);
        }
    }

    struct BrokenOutput {
        fail_on_flush: bool,
    }

    impl Write for BrokenOutput {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.fail_on_flush {
                Ok(bytes.len())
            } else {
                Err(io::ErrorKind::BrokenPipe.into())
            }
        }
        fn flush(&mut self) -> io::Result<()> {
            if self.fail_on_flush {
                Err(io::ErrorKind::BrokenPipe.into())
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn failed_output_does_not_advance_snapshot_high_water() {
        for fail_on_flush in [false, true] {
            let mut progress = Progress::default();
            let mut output = Output::from_writer(BrokenOutput { fail_on_flush }).unwrap();
            assert!(
                progress
                    .observe(hash(1), 10, true, 1, &mut output)
                    .await
                    .is_err()
            );
            assert_eq!(progress.snapshots, 0);
            assert_eq!(progress.last_number(), None);
        }
    }

    #[tokio::test]
    async fn retry_output_failure_is_terminal_not_another_connection() {
        let root = tempfile::tempdir().unwrap();
        let args = Args {
            socket: None,
            snapshots: 1,
            timeout_secs: 5,
            step_timeout_secs: 1,
            max_recoveries: 8,
        };
        for fail_on_flush in [false, true] {
            let mut holder = None;
            let mut progress = Progress::default();
            let mut budget = Budget::default();
            let mut output = Output::from_writer(BrokenOutput { fail_on_flush }).unwrap();
            tokio::time::timeout(
                Duration::from_secs(5),
                watch(
                    &args,
                    &root.path().join("absent.sock"),
                    &mut holder,
                    &mut progress,
                    &mut budget,
                    &mut output,
                ),
            )
            .await
            .expect("output failure must terminate instead of backing off again")
            .unwrap_err();
            assert_eq!(budget.used, 1);
            assert!(holder.is_none());
        }
    }

    #[tokio::test]
    async fn cancelling_retry_publication_leaves_no_session_or_later_attempt() {
        struct Release(std::sync::mpsc::Sender<()>);
        impl Drop for Release {
            fn drop(&mut self) {
                let _ = self.0.send(());
            }
        }
        struct HeldFlush {
            captured: Capture,
            entered: Option<tokio::sync::oneshot::Sender<()>>,
            release: std::sync::mpsc::Receiver<()>,
            stopped: Option<tokio::sync::oneshot::Sender<()>>,
        }
        impl Write for HeldFlush {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.captured.write(bytes)
            }
            fn flush(&mut self) -> io::Result<()> {
                let _ = self.entered.take().unwrap().send(());
                let _ = self.release.recv();
                Ok(())
            }
        }
        impl Drop for HeldFlush {
            fn drop(&mut self) {
                let _ = self.stopped.take().unwrap().send(());
            }
        }
        let root = tempfile::tempdir().unwrap();
        let socket = root.path().join("absent.sock");
        let args = Args {
            socket: None,
            snapshots: 1,
            timeout_secs: 5,
            step_timeout_secs: 1,
            max_recoveries: 8,
        };
        let captured = Capture::default();
        let (entered, writing) = tokio::sync::oneshot::channel();
        let (stopped, worker_stopped) = tokio::sync::oneshot::channel();
        let (release, waiting) = std::sync::mpsc::channel();
        let release = Release(release);
        let mut output = Output::from_writer(HeldFlush {
            captured: captured.clone(),
            entered: Some(entered),
            release: waiting,
            stopped: Some(stopped),
        })
        .unwrap();
        let mut holder = None;
        let mut progress = Progress::default();
        let mut budget = Budget::default();
        tokio::select! {
            result = watch(&args, &socket, &mut holder, &mut progress, &mut budget, &mut output) => {
                panic!("watch unexpectedly finished: {result:?}");
            }
            result = writing => result.unwrap(),
        }
        assert!(!output.usable());
        assert!(holder.is_none());
        assert_eq!(budget.used, 1);
        assert_eq!(progress.snapshots, 0);
        let records = records(&captured);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["event"], "retry");
        assert_eq!(records[0]["next_attempt"], 2);
        assert_eq!(records[0]["last_number"], Value::Null);
        drop(output);
        drop(release);
        tokio::time::timeout(Duration::from_secs(5), worker_stopped)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn cancelling_inflight_read_disposes_the_owned_session_without_reuse() {
        use futures_util::{SinkExt, StreamExt};
        use tokio::net::{UnixListener, UnixStream};
        use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

        async fn request(socket: &mut WebSocketStream<UnixStream>, method: &str) -> Value {
            let message = socket.next().await.unwrap().unwrap();
            let request: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
            assert_eq!(request["method"], method);
            request
        }
        async fn reply(socket: &mut WebSocketStream<UnixStream>, request: Value, result: Value) {
            socket
                .send(Message::Text(
                    json!({
                        "jsonrpc": "2.0", "id": request["id"], "result": result,
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
        }

        tokio::time::timeout(Duration::from_secs(5), async {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("rpc.sock");
            let listener = UnixListener::bind(&path).unwrap();
            let (sender, entered_storage) = tokio::sync::oneshot::channel();
            let peer = tokio::spawn(async move {
                let mut discovery = accept_async(listener.accept().await.unwrap().0).await.unwrap();
                let call = request(&mut discovery, "host_discover").await;
                reply(&mut discovery, call, json!({
                    "protocol_version": 1, "read_only": true,
                    "chains": [{
                        "id": "bulletin", "name": "test", "genesis_hash": GENESIS,
                        "relay_chain": null, "para_id": null, "properties": {}, "rpc_methods": [],
                    }],
                })).await;
                assert!(matches!(discovery.next().await.unwrap().unwrap(), Message::Close(_)));
                drop(discovery);
                let mut socket = accept_async(listener.accept().await.unwrap().0).await.unwrap();
                let call = request(&mut socket, "chainSpec_v1_genesisHash").await;
                reply(&mut socket, call, json!(GENESIS)).await;
                let call = request(&mut socket, "rpc_methods").await;
                reply(&mut socket, call, json!({"methods": [
                    "chainHead_v1_follow", "chainHead_v1_call", "chainHead_v1_storage",
                    "chainHead_v1_continue", "chainHead_v1_stopOperation",
                    "chainHead_v1_unpin", "chainHead_v1_unfollow",
                ]})).await;
                let call = request(&mut socket, "chainHead_v1_follow").await;
                reply(&mut socket, call, json!("subscription")).await;
                socket.send(Message::Text(json!({
                    "jsonrpc": "2.0", "method": "chainHead_v1_followEvent",
                    "params": {"subscription": "subscription", "result": {
                        "event": "initialized", "finalizedBlockHashes": [hash(1)],
                    }},
                }).to_string().into())).await.unwrap();
                request(&mut socket, "chainHead_v1_storage").await;
                sender.send(()).unwrap();
                // A cancelled RPC must not lead to more protocol work on this
                // poisoned transport, even when final disposal is attempted.
                assert!(matches!(socket.next().await.unwrap().unwrap(), Message::Close(_)));
            });
            let args = Args {
                socket: None, snapshots: 1, timeout_secs: 5,
                step_timeout_secs: 5, max_recoveries: 8,
            };
            let mut holder = None;
            let mut progress = Progress::default();
            let mut budget = Budget::default();
            let (mut output, captured) = captured_output();
            tokio::select! {
                result = watch(&args, &path, &mut holder, &mut progress, &mut budget, &mut output) => {
                    panic!("watch unexpectedly finished: {result:?}");
                }
                result = entered_storage => result.unwrap(),
            }
            assert!(holder.is_some(), "in-flight session must remain available for final disposal");
            assert!(dispose(&mut holder).await.is_err(), "poisoned protocol cleanup cannot succeed");
            assert!(holder.is_none());
            assert_eq!(progress.snapshots, 0);
            assert_eq!(budget.used, 0);
            assert_eq!(records(&captured)[0]["event"], "subscribed");
            peer.await.unwrap();
        }).await.unwrap();
    }
}
