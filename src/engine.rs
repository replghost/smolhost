// SPDX-License-Identifier: GPL-3.0-or-later

use std::{
    num::NonZeroU32,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use serde_json::value::RawValue;
use smoldot_light::{AddChainConfig, AddChainConfigJsonRpc, ChainId, Client, JsonRpcResponses};

use crate::platform::Platform;
pub type SharedEngine = Arc<Mutex<Engine>>;

pub struct Engine {
    client: Client<Platform>,
    specification: String,
    active_sessions: usize,
}

impl Engine {
    pub fn new(
        specification: String,
        database_content: Option<&str>,
    ) -> Result<(SharedEngine, String, Option<CheckpointClient>)> {
        let spec = smoldot::chain_spec::ChainSpec::from_json_bytes(&specification)
            .context("parsing chain specification")?;
        let chain = spec.id().to_owned();
        let block_number_bytes = usize::from(spec.block_number_bytes());
        drop(spec);
        ensure!(
            !chain.is_empty()
                && chain
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "chain id must contain only ASCII letters, digits, hyphens, or underscores"
        );
        let mut client = Client::new(Platform::new());
        // The anchor keeps sync alive without counting as an application session.
        // Its optional RPC frontend is private to checkpointing.
        let json_rpc = if database_content.is_some() {
            AddChainConfigJsonRpc::Enabled {
                max_pending_requests: NonZeroU32::new(1).unwrap(),
                max_subscriptions: 0,
            }
        } else {
            AddChainConfigJsonRpc::Disabled
        };
        let added = client
            .add_chain(config(
                &specification,
                database_content.unwrap_or(""),
                json_rpc,
            ))
            .map_err(|error| anyhow::anyhow!("adding chain: {error}"))?;
        let engine = Arc::new(Mutex::new(Self {
            client,
            specification,
            active_sessions: 0,
        }));
        let checkpoint = if database_content.is_some() {
            Some(CheckpointClient {
                engine: Arc::clone(&engine),
                chain_id: added.chain_id,
                responses: added
                    .json_rpc_responses
                    .context("missing checkpoint RPC response stream")?,
                block_number_bytes,
                next_request_id: 1,
                pending_request: None,
            })
        } else {
            None
        };
        log::info!("host: engine ready chain={chain}");
        Ok((engine, chain, checkpoint))
    }
}

fn config<'a>(
    specification: &'a str,
    database_content: &'a str,
    json_rpc: AddChainConfigJsonRpc,
) -> AddChainConfig<'a, (), std::iter::Empty<ChainId>> {
    AddChainConfig {
        specification,
        json_rpc,
        database_content,
        potential_relay_chains: std::iter::empty(),
        user_data: (),
        statement_protocol_config: None,
        max_pending_transactions: NonZeroU32::new(1).unwrap(),
    }
}

pub struct Snapshot {
    pub database: String,
    pub finalized_height: u64,
}

pub struct CheckpointClient {
    engine: SharedEngine,
    chain_id: ChainId,
    responses: JsonRpcResponses<Platform>,
    block_number_bytes: usize,
    next_request_id: u64,
    pending_request: Option<u64>,
}

#[derive(Deserialize)]
struct CheckpointResponse<'a> {
    id: u64,
    #[serde(borrow)]
    result: Option<&'a RawValue>,
    #[serde(borrow)]
    error: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct DatabaseFinality<'a> {
    #[serde(borrow)]
    chain: &'a RawValue,
}

impl CheckpointClient {
    pub async fn snapshot(&mut self) -> Result<Snapshot> {
        let request_id = self.next_request_id;
        self.next_request_id = request_id
            .checked_add(1)
            .context("checkpoint RPC request IDs exhausted")?;

        loop {
            // A caller's timeout can cancel this future, but not smoldot's request.
            // Drain that response before enqueueing another: the private frontend
            // allows only one request whose response has not yet been consumed.
            if self.pending_request.is_none() {
                let request = format!(
                    r#"{{"jsonrpc":"2.0","id":{request_id},"method":"chainHead_unstable_finalizedDatabase","params":[{}]}}"#,
                    crate::state::MAX_DATABASE_BYTES,
                );
                self.engine
                    .lock()
                    .expect("engine lock poisoned")
                    .client
                    .json_rpc_request(request, self.chain_id)
                    .map_err(|error| {
                        anyhow::anyhow!("checkpoint RPC queue rejected request: {error:?}")
                    })?;
                self.pending_request = Some(request_id);
            }
            let pending_id = self.pending_request.expect("checkpoint request is pending");
            let response = self
                .responses
                .next()
                .await
                .context("checkpoint RPC response stream closed")?;
            let response: CheckpointResponse<'_> = match serde_json::from_str(&response) {
                Ok(response) => response,
                Err(error) => {
                    self.pending_request = None;
                    return Err(error).context("invalid checkpoint RPC response");
                }
            };
            if response.id < pending_id {
                continue;
            }
            self.pending_request = None;
            ensure!(
                response.id == pending_id,
                "unexpected checkpoint RPC response ID"
            );
            if response.id != request_id {
                continue;
            }
            if let Some(error) = response.error {
                anyhow::bail!("checkpoint RPC failed: {}", error.get());
            }
            let result = response
                .result
                .context("checkpoint RPC result is missing")?;
            // Decode directly into the owned database string, without cloning a
            // JSON value or allocating the large database's unrelated fields.
            let database: String = serde_json::from_str(result.get())
                .context("checkpoint RPC result is not a string")?;
            return validate_snapshot(database, self.block_number_bytes);
        }
    }
}

fn validate_snapshot(database: String, block_number_bytes: usize) -> Result<Snapshot> {
    ensure!(
        database.len() <= crate::state::MAX_DATABASE_BYTES,
        "checkpoint database exceeds the size limit"
    );
    let finality: DatabaseFinality<'_> = serde_json::from_str(&database)
        .context("checkpoint database lacks valid finalized data")?;
    let decoded = smoldot::database::finalized_serialize::decode_chain(
        finality.chain.get(),
        block_number_bytes,
    )
    .context("checkpoint finalized chain information is invalid")?;
    let finalized_height = decoded
        .chain_information
        .as_ref()
        .finalized_block_header
        .number;
    Ok(Snapshot {
        database,
        finalized_height,
    })
}

pub struct Session {
    engine: SharedEngine,
    chain_id: ChainId,
    pub responses: JsonRpcResponses<Platform>,
}

impl Session {
    pub fn open(engine: SharedEngine) -> Result<Self> {
        let mut locked = engine.lock().expect("engine lock poisoned");
        let Engine {
            client,
            specification,
            active_sessions,
        } = &mut *locked;
        // Smoldot deduplicates chain services but creates a fresh RPC frontend.
        let added = client
            .add_chain(config(
                specification,
                "",
                AddChainConfigJsonRpc::Enabled {
                    max_pending_requests: NonZeroU32::new(32).unwrap(),
                    max_subscriptions: 8,
                },
            ))
            .map_err(|error| anyhow::anyhow!("adding session: {error}"))?;
        *active_sessions += 1;
        log::info!("host: session opened active={active_sessions}");
        drop(locked);
        Ok(Self {
            engine,
            chain_id: added.chain_id,
            responses: added
                .json_rpc_responses
                .context("missing RPC response stream")?,
        })
    }

    pub fn request(&self, request: String) -> Result<()> {
        self.engine
            .lock()
            .expect("engine lock poisoned")
            .client
            .json_rpc_request(request, self.chain_id)
            .map_err(|error| anyhow::anyhow!("RPC queue rejected request: {error:?}"))
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let mut engine = self.engine.lock().expect("engine lock poisoned");
        let () = engine.client.remove_chain(self.chain_id);
        engine.active_sessions -= 1;
        log::info!("host: session closed active={}", engine.active_sessions);
    }
}

#[cfg(test)]
mod tests {
    use super::validate_snapshot;
    use smoldot::{
        chain::chain_information::{
            ChainInformation, ChainInformationConsensus, ChainInformationFinality,
            ValidChainInformation,
        },
        database::finalized_serialize,
        header::{DigestItemRef, DigestRef, GrandpaConsensusLogRef, Header, HeaderRef},
    };

    #[test]
    fn rejects_unusable_finalized_databases() {
        for database in [
            "<too-large>",
            "",
            "not JSON",
            "{}",
            r#"{"chain":null}"#,
            r#"{"chain":{}}"#,
        ] {
            assert!(validate_snapshot(database.to_owned(), 4).is_err());
        }
        assert!(validate_snapshot(" ".repeat(crate::state::MAX_DATABASE_BYTES + 1), 4).is_err());
    }

    #[test]
    fn decodes_finalized_height_with_chain_block_number_width() {
        let height = u64::from(u32::MAX) + 1;
        let mut header = Header {
            parent_hash: [0; 32],
            number: height,
            state_root: [0; 32],
            extrinsics_root: [0; 32],
            digest: DigestRef::empty().into(),
        };
        // Header numbers are compact-encoded; a GRANDPA digest exercises the
        // chain-specific fixed-width block number encoding.
        for item in [
            DigestItemRef::GrandpaConsensus(GrandpaConsensusLogRef::Pause(height)),
            DigestItemRef::AuraPreDigest(smoldot::header::AuraPreDigest { slot_number: 1 }),
            DigestItemRef::AuraSeal(&[0; 64]),
        ] {
            let mut encoded_header = Vec::new();
            for part in HeaderRef::from(&header).scale_encoding_with_extra_digest_item(8, item) {
                encoded_header.extend_from_slice(part.as_ref());
            }
            header.digest = smoldot::header::decode(&encoded_header, 8)
                .unwrap()
                .digest
                .into();
        }
        let information = ValidChainInformation::try_from(ChainInformation {
            finalized_block_header: Box::new(header),
            consensus: ChainInformationConsensus::Aura {
                finalized_authorities_list: vec![smoldot::header::AuraAuthority {
                    public_key: [0; 32],
                }],
                slot_duration: std::num::NonZeroU64::new(6000).unwrap(),
            },
            finality: ChainInformationFinality::Outsourced,
        })
        .unwrap();
        let encoded = finalized_serialize::encode_chain(&information, 8);
        let database = format!(r#"{{"chain":{encoded}}}"#);
        let snapshot = validate_snapshot(database, 8).unwrap();
        assert_eq!(snapshot.finalized_height, height);
        assert_eq!(snapshot.database, format!(r#"{{"chain":{encoded}}}"#));
        assert!(validate_snapshot(snapshot.database, 4).is_err());
    }
}
