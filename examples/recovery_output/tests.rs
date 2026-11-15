// SPDX-License-Identifier: GPL-3.0-or-later

use super::*;
use serde_json::json;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Condvar, Mutex},
};
use tokio::sync::Notify;

const FIXTURE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy)]
enum Mode {
    Plain,
    PartialWriteBlocked,
    FlushBlocked,
    WriteBrokenPipe,
    PartialBrokenPipe,
    FlushBrokenPipe,
}

#[derive(Clone, Default)]
struct Capture {
    bytes: Vec<u8>,
    writes: usize,
    flushes: usize,
}

#[derive(Default)]
struct Shared {
    capture: Mutex<Capture>,
    released: Mutex<bool>,
    release: Condvar,
    blocked: Notify,
    dropped: Notify,
}

struct Fixture(Arc<Shared>);

impl Fixture {
    fn new(mode: Mode) -> (Self, RecordingWriter) {
        let shared = Arc::new(Shared::default());
        (
            Self(shared.clone()),
            RecordingWriter {
                shared,
                mode,
                first_write: true,
                already_blocked: false,
            },
        )
    }

    fn capture(&self) -> Capture {
        self.0.capture.lock().unwrap().clone()
    }

    fn release(&self) {
        *self.0.released.lock().unwrap() = true;
        self.0.release.notify_all();
    }

    async fn wait_blocked(&self, emission: Pin<&mut impl Future<Output = Result<()>>>) {
        tokio::time::timeout(FIXTURE_TIMEOUT, async {
            tokio::select! {
                biased;
                result = emission => panic!("emit completed before fixture release: {result:?}"),
                () = self.0.blocked.notified() => {}
            }
        })
        .await
        .expect("writer did not reach its blocking point");
    }

    // Call after dropping Output so the detached worker must also exit. This
    // observes writer destruction, rather than leaving cleanup to process exit.
    async fn finish(self) -> Capture {
        self.release();
        tokio::time::timeout(FIXTURE_TIMEOUT, self.0.dropped.notified())
            .await
            .expect("released output writer did not exit");
        self.capture()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Assertion failures and cancellation must release synthetic I/O too.
        self.release();
    }
}

struct RecordingWriter {
    shared: Arc<Shared>,
    mode: Mode,
    first_write: bool,
    already_blocked: bool,
}

impl RecordingWriter {
    fn block_once(&mut self) -> io::Result<()> {
        if self.already_blocked {
            return Ok(());
        }
        self.already_blocked = true;
        self.shared.blocked.notify_one();
        let (released, _) = self
            .shared
            .release
            .wait_timeout_while(
                self.shared.released.lock().unwrap(),
                FIXTURE_TIMEOUT,
                |released| !*released,
            )
            .unwrap();
        if !*released {
            // A fixture bug must not leave a permanent thread in the test runner.
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "test fixture was not released",
            ));
        }
        Ok(())
    }
}

impl Write for RecordingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.shared.capture.lock().unwrap().writes += 1;
        if matches!(self.mode, Mode::WriteBrokenPipe) {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe));
        }
        if self.first_write
            && matches!(
                self.mode,
                Mode::PartialWriteBlocked | Mode::PartialBrokenPipe
            )
        {
            self.first_write = false;
            let length = bytes.len().min(1);
            self.shared
                .capture
                .lock()
                .unwrap()
                .bytes
                .extend_from_slice(&bytes[..length]);
            return Ok(length);
        }
        match self.mode {
            Mode::PartialWriteBlocked => self.block_once()?,
            Mode::PartialBrokenPipe => return Err(io::Error::from(io::ErrorKind::BrokenPipe)),
            _ => {}
        }
        self.shared
            .capture
            .lock()
            .unwrap()
            .bytes
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.mode {
            Mode::FlushBlocked => self.block_once()?,
            Mode::FlushBrokenPipe => return Err(io::Error::from(io::ErrorKind::BrokenPipe)),
            _ => {}
        }
        self.shared.capture.lock().unwrap().flushes += 1;
        Ok(())
    }
}

impl Drop for RecordingWriter {
    fn drop(&mut self) {
        self.shared.dropped.notify_one();
    }
}

#[tokio::test]
async fn exact_limit_includes_newline_and_recycles_one_allocation() {
    let (fixture, writer) = Fixture::new(Mode::Plain);
    let mut output = Output::from_writer(writer).unwrap();
    let buffer = &output.ready.as_ref().unwrap().buffer;
    let pointer = buffer.as_ptr();
    assert_eq!(buffer.capacity(), MAX_RECORD_BYTES);
    let mut expected = Vec::new();
    for record in [
        json!("x".repeat(MAX_RECORD_BYTES - 3)),
        Value::Null,
        json!({"ok": true}),
    ] {
        serde_json::to_writer(&mut expected, &record).unwrap();
        expected.push(b'\n');
        output.emit(record).await.unwrap();
        assert!(output.usable());
        let buffer = &output.ready.as_ref().unwrap().buffer;
        assert_eq!(buffer.as_ptr(), pointer);
        assert_eq!(buffer.capacity(), MAX_RECORD_BYTES);
        assert!(buffer.is_empty());
    }
    assert_eq!(expected[MAX_RECORD_BYTES - 1], b'\n');
    drop(output);
    let capture = fixture.finish().await;
    assert_eq!(capture.bytes, expected);
    assert_eq!(capture.flushes, 3);
}

#[test]
fn saturated_serialization_rejects_without_growing_the_buffer() {
    for record in [
        json!("x".repeat(MAX_RECORD_BYTES)),
        json!("\n".repeat(MAX_RECORD_BYTES)),
        json!({"large": "é".repeat(MAX_RECORD_BYTES)}),
    ] {
        let mut buffer = Vec::with_capacity(MAX_RECORD_BYTES);
        let pointer = buffer.as_ptr();
        let result = serde_json::to_writer(
            BoundedBuffer {
                buffer: &mut buffer,
                limit: MAX_RECORD_BYTES - 1,
            },
            &record,
        );
        assert!(result.is_err());
        assert!(buffer.len() < MAX_RECORD_BYTES);
        assert_eq!(buffer.capacity(), MAX_RECORD_BYTES);
        assert_eq!(buffer.as_ptr(), pointer);
    }
    let mut buffer = Vec::with_capacity(MAX_RECORD_BYTES);
    let mut bounded = BoundedBuffer {
        buffer: &mut buffer,
        limit: MAX_RECORD_BYTES - 1,
    };
    bounded.write_all(&[b'x'; MAX_RECORD_BYTES - 1]).unwrap();
    assert!(bounded.write_all(b"x").is_err());
    assert_eq!(buffer.len(), MAX_RECORD_BYTES - 1);
    assert_eq!(buffer.capacity(), MAX_RECORD_BYTES);
}

#[tokio::test]
async fn oversized_record_is_terminal_before_any_bytes_are_sent() {
    let (fixture, writer) = Fixture::new(Mode::Plain);
    let mut output = Output::from_writer(writer).unwrap();
    assert!(
        output
            .emit(json!("x".repeat(MAX_RECORD_BYTES - 2)))
            .await
            .is_err()
    );
    assert!(!output.usable());
    assert!(output.emit(Value::Null).await.is_err());
    drop(output);
    let capture = fixture.finish().await;
    assert!(capture.bytes.is_empty());
    assert_eq!(capture.writes, 0);
    assert_eq!(capture.flushes, 0);
}

#[tokio::test]
async fn acknowledgement_requires_all_bytes_and_flush() {
    for mode in [Mode::PartialWriteBlocked, Mode::FlushBlocked] {
        let (fixture, writer) = Fixture::new(mode);
        let mut output = Output::from_writer(writer).unwrap();
        {
            let emission = output.emit(json!({"record": 1}));
            tokio::pin!(emission);
            fixture.wait_blocked(emission.as_mut()).await;
            assert!(futures_util::poll!(emission.as_mut()).is_pending());
            let capture = fixture.capture();
            assert_eq!(capture.flushes, 0);
            match mode {
                Mode::PartialWriteBlocked => assert_eq!(capture.bytes, b"{"),
                Mode::FlushBlocked => assert_eq!(capture.bytes, b"{\"record\":1}\n"),
                _ => unreachable!(),
            }
            fixture.release();
            tokio::time::timeout(FIXTURE_TIMEOUT, emission)
                .await
                .unwrap()
                .unwrap();
        }
        assert!(output.usable());
        drop(output);
        let capture = fixture.finish().await;
        assert_eq!(capture.bytes, b"{\"record\":1}\n");
        assert_eq!(capture.flushes, 1);
    }
}

#[tokio::test]
async fn cancelling_partial_or_unflushed_output_forbids_another_record() {
    for mode in [Mode::PartialWriteBlocked, Mode::FlushBlocked] {
        let (fixture, writer) = Fixture::new(mode);
        let mut output = Output::from_writer(writer).unwrap();
        {
            let emission = output.emit(json!({"first": true}));
            tokio::pin!(emission);
            fixture.wait_blocked(emission.as_mut()).await;
        }
        assert!(!output.usable());
        assert!(output.emit(json!({"second": true})).await.is_err());
        let start = std::time::Instant::now();
        drop(output);
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "drop waited for blocked I/O"
        );
        let capture = fixture.finish().await;
        assert_eq!(capture.bytes, b"{\"first\":true}\n");
        assert_eq!(capture.flushes, 1);
    }
}

#[tokio::test]
async fn output_timeout_poisons_the_stream_without_waiting_for_the_writer() {
    let (fixture, writer) = Fixture::new(Mode::PartialWriteBlocked);
    let mut output = Output::from_writer(writer).unwrap();
    let error = {
        let emission = output.emit(json!({"first": true}));
        tokio::pin!(emission);
        fixture.wait_blocked(emission.as_mut()).await;
        tokio::time::timeout(OUTPUT_TIMEOUT + Duration::from_secs(1), emission)
            .await
            .expect("emit ignored its output deadline")
            .unwrap_err()
    };
    assert!(
        error
            .downcast_ref::<tokio::time::error::Elapsed>()
            .is_some()
    );
    assert!(!output.usable());
    assert_eq!(fixture.capture().bytes, b"{");
    assert!(output.emit(Value::Null).await.is_err());
    drop(output);
    let capture = fixture.finish().await;
    assert_eq!(capture.bytes, b"{\"first\":true}\n");
    assert_eq!(capture.flushes, 1);
}

#[tokio::test]
async fn write_partial_write_and_flush_broken_pipe_are_terminal() {
    for mode in [
        Mode::WriteBrokenPipe,
        Mode::PartialBrokenPipe,
        Mode::FlushBrokenPipe,
    ] {
        let (fixture, writer) = Fixture::new(mode);
        let mut output = Output::from_writer(writer).unwrap();
        let error = output.emit(json!({"first": true})).await.unwrap_err();
        assert_eq!(
            error.downcast_ref::<io::Error>().unwrap().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert!(!output.usable());
        let writes = fixture.capture().writes;
        assert!(output.emit(Value::Null).await.is_err());
        drop(output);
        let capture = fixture.finish().await;
        assert_eq!(capture.writes, writes);
        assert_eq!(capture.flushes, 0);
        match mode {
            Mode::WriteBrokenPipe => assert!(capture.bytes.is_empty()),
            Mode::PartialBrokenPipe => assert_eq!(capture.bytes, b"{"),
            Mode::FlushBrokenPipe => assert_eq!(capture.bytes, b"{\"first\":true}\n"),
            _ => unreachable!(),
        }
    }
}

#[tokio::test]
async fn diagnostics_are_bounded_and_truncation_preserves_utf8() {
    let (fixture, writer) = Fixture::new(Mode::Plain);
    let mut output = Output::from_writer(writer).unwrap();
    let mut offset = 0;
    for message in [
        String::new(),
        "x".repeat(MAX_RECORD_BYTES - 1),
        "a".to_owned() + &"水".repeat(MAX_RECORD_BYTES),
    ] {
        output
            .diagnostic(&message, Instant::now() + STDERR_TIMEOUT)
            .await
            .unwrap();
        let capture = fixture.capture();
        let emitted = &capture.bytes[offset..];
        assert!(emitted.len() <= MAX_RECORD_BYTES);
        assert!(std::str::from_utf8(emitted).is_ok());
        if message.len() < MAX_RECORD_BYTES {
            assert_eq!(&emitted[..emitted.len() - 1], message.as_bytes());
            assert_eq!(emitted.last(), Some(&b'\n'));
        } else {
            assert!(emitted.ends_with(TRUNCATED));
        }
        offset = capture.bytes.len();
        assert_eq!(
            output.ready.as_ref().unwrap().buffer.capacity(),
            MAX_RECORD_BYTES
        );
    }
    drop(output);
    assert_eq!(fixture.finish().await.flushes, 3);
}

#[tokio::test]
async fn blocked_diagnostic_uses_the_short_deadline_and_can_be_dropped() {
    let (fixture, writer) = Fixture::new(Mode::FlushBlocked);
    let mut output = Output::from_writer(writer).unwrap();
    let error = {
        let emission = output.diagnostic("failure", Instant::now() + STDERR_TIMEOUT);
        tokio::pin!(emission);
        fixture.wait_blocked(emission.as_mut()).await;
        tokio::time::timeout(Duration::from_secs(1), emission)
            .await
            .expect("diagnostic used the longer JSONL deadline")
            .unwrap_err()
    };
    assert!(
        error
            .downcast_ref::<tokio::time::error::Elapsed>()
            .is_some()
    );
    assert!(!output.usable());
    assert_eq!(fixture.capture().flushes, 0);
    drop(output);
    let capture = fixture.finish().await;
    assert_eq!(capture.bytes, b"failure\n");
    assert_eq!(capture.flushes, 1);
}
