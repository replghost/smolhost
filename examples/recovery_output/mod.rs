// SPDX-License-Identifier: GPL-3.0-or-later

//! Bounded, acknowledged output for the standalone recovery example.
//!
//! A single detached OS thread owns the raw writer. In particular, this does not
//! use Tokio's blocking pool (whose shutdown waits for blocked writes), global
//! stdio locks, or changes to inherited open-file flags. Duplicated descriptors
//! retain the original open-file description's flags and offset semantics.
//!
//! There is one recycled record buffer and one slot in each channel. An emit is
//! committed only after both write_all and flush succeed. Cancellation, timeout,
//! or any error permanently poisons the output: an uncertain prefix must never
//! be followed by another JSONL record. Dropping Output only closes channels; it
//! never joins the thread. A permanently blocked write can therefore outlive this
//! object, until the standalone process exits and the OS closes its descriptors.
//! This is an intentional process boundary, not a general-purpose cancellable
//! writer for long-lived libraries.

use std::{
    fs::File,
    io::{self, Write},
    sync::mpsc::{self, SyncSender},
    thread,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde_json::Value;
use tokio::{sync::mpsc as async_mpsc, time::Instant};

pub const MAX_RECORD_BYTES: usize = 4096;
pub const OUTPUT_TIMEOUT: Duration = Duration::from_secs(2);
const STDERR_TIMEOUT: Duration = Duration::from_millis(100);
const TRUNCATED: &[u8] = b"... [truncated]\n";

struct Ready {
    requests: SyncSender<Vec<u8>>,
    buffer: Vec<u8>,
}

struct Acknowledgement {
    buffer: Vec<u8>,
    result: io::Result<()>,
}

pub struct Output {
    // Taking this before serialization makes every early return and cancellation
    // terminal. The in-flight future owns the sole request sender, so cancelling
    // it also tells the worker to exit as soon as its current write returns.
    ready: Option<Ready>,
    acknowledgements: async_mpsc::Receiver<Acknowledgement>,
}

impl Output {
    pub fn from_writer(mut writer: impl Write + Send + 'static) -> Result<Self> {
        let (requests, incoming) = mpsc::sync_channel::<Vec<u8>>(1);
        let (completed, acknowledgements) = async_mpsc::channel(1);
        let buffer = Vec::with_capacity(MAX_RECORD_BYTES);
        let worker = thread::Builder::new()
            .name("recovery-output".to_owned())
            .spawn(move || {
                while let Ok(buffer) = incoming.recv() {
                    let result = writer.write_all(&buffer).and_then(|()| writer.flush());
                    let failed = result.is_err();
                    if completed
                        .try_send(Acknowledgement { buffer, result })
                        .is_err()
                        || failed
                    {
                        break;
                    }
                }
            })
            .context("starting output writer thread")?;
        // Intentionally detach: joining a blocked writer would defeat deadlines.
        drop(worker);
        Ok(Self {
            ready: Some(Ready { requests, buffer }),
            acknowledgements,
        })
    }

    pub fn stdout() -> Result<Self> {
        let descriptor =
            rustix::io::dup(io::stdout()).context("duplicating stdout for bounded output")?;
        Self::from_writer(File::from(descriptor))
    }

    pub fn usable(&self) -> bool {
        self.ready.is_some()
    }

    pub async fn emit(&mut self, record: Value) -> Result<()> {
        let deadline = Instant::now() + OUTPUT_TIMEOUT;
        let mut ready = self
            .ready
            .take()
            .context("output is permanently poisoned")?;
        serde_json::to_writer(
            BoundedBuffer {
                buffer: &mut ready.buffer,
                limit: MAX_RECORD_BYTES - 1,
            },
            &record,
        )
        .context("serializing bounded JSONL record")?;
        ready.buffer.push(b'\n');
        drop(record);
        self.send(ready, deadline).await
    }

    async fn send(&mut self, ready: Ready, deadline: Instant) -> Result<()> {
        let Ready { requests, buffer } = ready;
        if requests.try_send(buffer).is_err() {
            bail!("output writer is unavailable or its request slot is occupied");
        }
        let acknowledgement = tokio::time::timeout_at(deadline, self.acknowledgements.recv())
            .await
            .context("timed out waiting for output write and flush")?
            .context("output writer stopped without acknowledging write and flush")?;
        acknowledgement
            .result
            .context("writing and flushing output")?;
        let mut buffer = acknowledgement.buffer;
        buffer.clear();
        self.ready = Some(Ready { requests, buffer });
        Ok(())
    }

    async fn diagnostic(&mut self, message: &str, deadline: Instant) -> Result<()> {
        let mut ready = self
            .ready
            .take()
            .context("output is permanently poisoned")?;
        if message.len() < MAX_RECORD_BYTES {
            ready.buffer.extend_from_slice(message.as_bytes());
            ready.buffer.push(b'\n');
        } else {
            let mut end = MAX_RECORD_BYTES - TRUNCATED.len();
            while !message.is_char_boundary(end) {
                end -= 1;
            }
            ready.buffer.extend_from_slice(&message.as_bytes()[..end]);
            ready.buffer.extend_from_slice(TRUNCATED);
        }
        self.send(ready, deadline).await
    }
}

/// Best-effort diagnostic: no global stderr lock and at most 100ms awaiting I/O.
/// Like stdout, a blocked worker is detached rather than joined on return/drop.
pub async fn report_stderr(message: &str) -> Result<()> {
    let deadline = Instant::now() + STDERR_TIMEOUT;
    let descriptor =
        rustix::io::dup(io::stderr()).context("duplicating stderr for bounded diagnostic")?;
    let mut output = Output::from_writer(File::from(descriptor))?;
    output.diagnostic(message, deadline).await
}

struct BoundedBuffer<'a> {
    buffer: &'a mut Vec<u8>,
    limit: usize,
}

impl Write for BoundedBuffer<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit - self.buffer.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "JSONL record exceeds 4096 bytes including newline",
            ));
        }
        self.buffer.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
