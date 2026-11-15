// SPDX-License-Identifier: GPL-3.0-or-later

//! Bounded, single-operation chainHead consumer shared by read-only examples.
//! RpcClient bounds notification buffering; this layer bounds pins and finalized hashes.

pub mod metadata;

#[cfg(test)]
mod tests;

use std::{
    collections::{HashSet, VecDeque},
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use smolhost::client::RpcClient;

pub const CODE_KEY: &str = "0x3a636f6465";
pub const CLEANUP_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_PINS: usize = 128;

/// Interrupted work; the caller decides whether to open a new session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Interruption {
    OperationInaccessible,
    FollowStopped,
}

impl std::fmt::Display for Interruption {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::OperationInaccessible => "verified chain operation inaccessible",
            Self::FollowStopped => "chainHead follow stopped",
        })
    }
}

impl std::error::Error for Interruption {}

pub struct Session {
    client: RpcClient,
    subscription: Option<String>,
    operation: Option<String>,
    pinned: HashSet<String>,
    pending: VecDeque<String>,
    obsolete: HashSet<String>,
}

impl Session {
    pub async fn initialize(&mut self) -> Result<()> {
        let methods = self.client.call("rpc_methods", json!([])).await?;
        let methods = methods["methods"]
            .as_array()
            .context("rpc_methods has no methods array")?;
        for required in [
            "chainHead_v1_follow",
            "chainHead_v1_call",
            "chainHead_v1_storage",
            "chainHead_v1_continue",
            "chainHead_v1_stopOperation",
            "chainHead_v1_unpin",
            "chainHead_v1_unfollow",
        ] {
            ensure!(
                methods.iter().any(|method| method == required),
                "daemon lacks {required}"
            );
        }
        let subscription = self
            .client
            .call("chainHead_v1_follow", json!([true]))
            .await?;
        self.subscription = Some(
            subscription
                .as_str()
                .context("follow subscription is not a string")?
                .to_owned(),
        );
        let initialized = self.event().await?;
        ensure!(
            initialized["event"] == "initialized",
            "expected initialized event: {initialized}"
        );
        let initial = block_hashes(&initialized, "finalizedBlockHashes")?;
        let latest = initial
            .last()
            .context("initialized has no finalized block")?
            .clone();
        for hash in initial {
            self.pin(hash.clone())?;
            if hash != latest {
                self.obsolete.insert(hash);
            }
        }
        self.pending.push_back(latest);
        self.release_obsolete().await?;

        Ok(())
    }

    pub async fn release(&mut self, block: String) -> Result<()> {
        self.obsolete.insert(block);
        self.release_obsolete().await
    }

    pub fn new(client: RpcClient) -> Self {
        Self {
            client,
            subscription: None,
            operation: None,
            pinned: HashSet::new(),
            pending: VecDeque::new(),
            obsolete: HashSet::new(),
        }
    }

    fn pin(&mut self, hash: String) -> Result<()> {
        ensure!(
            self.pinned.len() < MAX_PINS,
            "follow pin backlog reached {MAX_PINS}; consumer cannot keep up"
        );
        ensure!(
            self.pinned.insert(hash),
            "duplicate pinned block announcement"
        );
        Ok(())
    }

    async fn event(&mut self) -> Result<Value> {
        let mut notification = self.client.next_notification().await?;
        ensure!(
            notification["method"] == "chainHead_v1_followEvent",
            "unexpected notification: {notification}"
        );
        ensure!(
            notification["params"]["subscription"].as_str() == self.subscription.as_deref(),
            "unexpected follow subscription"
        );
        let event = notification["params"]["result"].take();
        if event["event"] == "stop" {
            self.subscription = None;
            self.operation = None;
            self.pinned.clear();
            self.pending.clear();
            self.obsolete.clear();
            return Err(Interruption::FollowStopped.into());
        }
        Ok(event)
    }

    // Interleaved follow events are applied immediately instead of retaining large
    // runtime descriptions. At most 128 hashes are pinned/queued, with one operation.
    fn follow_event(&mut self, event: &Value) -> Result<()> {
        match string(event, "event")? {
            "newBlock" => {
                let hash = string(event, "blockHash")?;
                validate_hash(hash)?;
                self.pin(hash.to_owned())?;
            }
            "bestBlockChanged" => {}
            "finalized" => {
                let finalized = block_hashes(event, "finalizedBlockHashes")?;
                ensure!(!finalized.is_empty(), "empty finalized event");
                for hash in finalized {
                    ensure!(
                        self.pinned.contains(&hash),
                        "finalized block was not announced: {hash}"
                    );
                    ensure!(
                        !self.pending.contains(&hash) && !self.obsolete.contains(&hash),
                        "duplicate finalization: {hash}"
                    );
                    ensure!(
                        self.pending.len() < MAX_PINS,
                        "finalized queue exceeds {MAX_PINS} blocks"
                    );
                    self.pending.push_back(hash);
                }
                for hash in block_hashes(event, "prunedBlockHashes")? {
                    ensure!(
                        !self.pending.contains(&hash),
                        "finalized block was also pruned"
                    );
                    if self.pinned.contains(&hash) {
                        self.obsolete.insert(hash);
                    }
                }
            }
            _ => bail!("unexpected follow event: {event}"),
        }
        Ok(())
    }

    pub async fn next_finalized(&mut self) -> Result<String> {
        loop {
            if let Some(block) = self.pending.pop_front() {
                return Ok(block);
            }
            let event = self.event().await?;
            self.follow_event(&event)?;
            self.release_obsolete().await?;
        }
    }

    async fn operation_event(&mut self) -> Result<Value> {
        loop {
            let event = self.event().await?;
            let kind = string(&event, "event")?;
            if !kind.starts_with("operation") {
                self.follow_event(&event)?;
                continue;
            }
            // smoldot's waiting event has no operationId; there is exactly one
            // operation in this application. Accept an ID only if it matches.
            if kind == "operationWaitingForContinue" {
                if event.get("operationId").is_some() {
                    self.check_operation(&event)?;
                }
                let result = self
                    .client
                    .call(
                        "chainHead_v1_continue",
                        json!([self.subscription, self.operation]),
                    )
                    .await?;
                ensure!(result.is_null(), "unexpected continue response: {result}");
                continue;
            }
            self.check_operation(&event)?;
            if matches!(kind, "operationInaccessible" | "operationError") {
                self.operation = None;
                if kind == "operationInaccessible" {
                    return Err(Interruption::OperationInaccessible.into());
                }
                bail!("verified chain operation failed: {event}");
            }
            return Ok(event);
        }
    }

    fn check_operation(&self, event: &Value) -> Result<()> {
        ensure!(
            event["operationId"].as_str() == self.operation.as_deref(),
            "unexpected operation ID: {event}"
        );
        Ok(())
    }

    fn started(&mut self, result: &Value) -> Result<()> {
        ensure!(
            result["result"] == "started",
            "operation refused (including limitReached; no retry): {result}"
        );
        self.operation = Some(string(result, "operationId")?.to_owned());
        Ok(())
    }

    pub async fn storage(&mut self, block: &str, key: &str, kind: &str) -> Result<Option<String>> {
        let result = self
            .client
            .call(
                "chainHead_v1_storage",
                json!([self.subscription, block, [{"key": key, "type": kind}], null]),
            )
            .await?;
        self.started(&result)?;
        ensure!(
            result["discardedItems"].as_u64() == Some(0),
            "storage item discarded: {result}"
        );
        let mut seen = false;
        let mut found = None;
        loop {
            let event = self.operation_event().await?;
            match string(&event, "event")? {
                "operationStorageItems" => {
                    let items = event["items"]
                        .as_array()
                        .context("storage items is not an array")?;
                    for item in items {
                        ensure!(item["key"] == key, "unexpected storage key: {item}");
                        ensure!(!seen, "duplicate storage result for {key}");
                        seen = true;
                        // Absent entries may be omitted entirely, or have a null/
                        // absent value. Completion, not an empty event, proves absence.
                        found = match item.get(kind) {
                            None | Some(Value::Null) => None,
                            Some(Value::String(value)) => Some(value.clone()),
                            _ => bail!("malformed storage {kind}: {item}"),
                        };
                        for other in ["value", "hash", "closestDescendantMerkleValue"] {
                            ensure!(
                                other == kind || item.get(other).is_none(),
                                "unexpected storage response field {other}"
                            );
                        }
                    }
                }
                "operationStorageDone" => {
                    self.operation = None;
                    return Ok(found);
                }
                _ => bail!("unexpected storage operation event: {event}"),
            }
        }
    }

    pub async fn runtime_metadata(&mut self, block: &str) -> Result<String> {
        let result = self
            .client
            .call(
                "chainHead_v1_call",
                json!([self.subscription, block, "Metadata_metadata", "0x"]),
            )
            .await?;
        self.started(&result)?;
        let event = self.operation_event().await?;
        ensure!(
            event["event"] == "operationCallDone",
            "unexpected metadata operation event: {event}"
        );
        self.operation = None;
        Ok(string(&event, "output")?.to_owned())
    }

    async fn release_obsolete(&mut self) -> Result<()> {
        if self.obsolete.is_empty() {
            return Ok(());
        }
        let hashes: Vec<_> = self.obsolete.iter().collect();
        let result = self
            .client
            .call("chainHead_v1_unpin", json!([self.subscription, hashes]))
            .await?;
        ensure!(result.is_null(), "unexpected unpin response: {result}");
        for hash in self.obsolete.drain() {
            self.pinned.remove(&hash);
        }
        Ok(())
    }

    pub async fn cleanup(&mut self) -> Result<()> {
        let protocol = async {
            if self.subscription.is_some() {
                if let Some(operation) = self.operation.take() {
                    let result = self
                        .client
                        .call(
                            "chainHead_v1_stopOperation",
                            json!([self.subscription, operation]),
                        )
                        .await?;
                    ensure!(
                        result.is_null(),
                        "unexpected stopOperation response: {result}"
                    );
                }
                self.obsolete.extend(self.pinned.iter().cloned());
                self.release_obsolete().await?;
                let result = self
                    .client
                    .call("chainHead_v1_unfollow", json!([self.subscription]))
                    .await?;
                ensure!(result.is_null(), "unexpected unfollow response: {result}");
                self.subscription = None;
            }
            Ok(())
        }
        .await;
        let close = self.client.close().await;
        protocol.and(close)
    }
}

fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field]
        .as_str()
        .with_context(|| format!("missing string {field}: {value}"))
}

pub fn validate_hash(hash: &str) -> Result<()> {
    ensure!(
        hash.len() == 66
            && hash.starts_with("0x")
            && hash.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit),
        "invalid 32-byte hash {hash}"
    );
    Ok(())
}

fn block_hashes(event: &Value, field: &str) -> Result<Vec<String>> {
    let values = event[field]
        .as_array()
        .with_context(|| format!("missing array {field}"))?;
    ensure!(
        values.len() <= MAX_PINS,
        "{field} exceeds {MAX_PINS} hashes"
    );
    values
        .iter()
        .map(|value| {
            let hash = value.as_str().context("block hash is not a string")?;
            validate_hash(hash)?;
            Ok(hash.to_owned())
        })
        .collect()
}
