// SPDX-License-Identifier: GPL-3.0-or-later

use std::{
    collections::{BTreeMap, HashSet},
    num::NonZeroU32,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use serde_json::{Value, json, value::RawValue};
use smoldot_light::{AddChainConfig, AddChainConfigJsonRpc, ChainId, Client, JsonRpcResponses};

use crate::{
    discovery::{ChainInfo, HostInfo, PROTOCOL_VERSION},
    ipc::{MAX_PENDING_REQUESTS, MAX_RESPONSE_BYTES, MAX_SUBSCRIPTIONS, allowed_method},
    platform::Platform,
};
pub type SharedEngine = Arc<Mutex<Engine>>;

pub struct Engine {
    client: Client<Platform>,
    chains: BTreeMap<String, ConfiguredChain>,
    active_sessions: usize,
}

struct ConfiguredChain {
    specification: String,
    relay_anchor: Option<ChainId>,
    // Only session handles are removed; anchors live until the client is dropped.
    _anchor_id: ChainId,
}

impl Engine {
    pub fn new(
        specification: String,
        database_content: Option<&str>,
        parachain_specifications: Vec<String>,
    ) -> Result<(SharedEngine, Vec<String>, Option<CheckpointClient>)> {
        // Reject the entire configuration before creating any chain services.
        let (chains, block_number_bytes) =
            validate_specifications(&specification, &parachain_specifications)?;
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
                None,
            ))
            .map_err(|error| anyhow::anyhow!("adding relay chain: {error}"))?;
        let relay_anchor = added.chain_id;
        let mut configured = BTreeMap::new();
        configured.insert(
            chains[0].clone(),
            ConfiguredChain {
                specification,
                relay_anchor: None,
                _anchor_id: relay_anchor,
            },
        );
        for (chain, specification) in chains.iter().skip(1).zip(parachain_specifications) {
            let anchor = client
                .add_chain(config(
                    &specification,
                    "",
                    AddChainConfigJsonRpc::Disabled,
                    Some(relay_anchor),
                ))
                .map_err(|error| anyhow::anyhow!("adding parachain {chain}: {error}"))?;
            configured.insert(
                chain.clone(),
                ConfiguredChain {
                    specification,
                    relay_anchor: Some(relay_anchor),
                    _anchor_id: anchor.chain_id,
                },
            );
        }
        let engine = Arc::new(Mutex::new(Self {
            client,
            chains: configured,
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
        for chain in &chains {
            log::info!("host: engine ready chain={chain}");
        }
        Ok((engine, chains, checkpoint))
    }
}

/// Read immutable identity through private startup frontends, never application RPC.
/// Every frontend is removed before returning, including on error or cancellation.
pub async fn catalog(engine: &SharedEngine, chains: &[String]) -> Result<HostInfo> {
    let mut entries = Vec::with_capacity(chains.len());
    for chain in chains {
        let (relay_chain, para_id) = {
            let locked = engine.lock().expect("engine lock poisoned");
            let configured = locked.chains.get(chain).context("unknown chain route")?;
            // This metadata has already passed validate_specifications.
            let metadata: ParachainMetadata<'_> = serde_json::from_str(&configured.specification)?;
            (
                metadata
                    .relay_chain
                    .map(|raw| serde_json::from_str(raw.get()))
                    .transpose()?,
                metadata
                    .para_id
                    .map(|raw| serde_json::from_str(raw.get()))
                    .transpose()?,
            )
        };
        let mut session = Session::open_inner(Arc::clone(engine), chain, false)?;
        let name =
            serde_json::from_value(startup_query(&mut session, "chainSpec_v1_chainName").await?)?;
        let genesis_hash: String =
            serde_json::from_value(startup_query(&mut session, "chainSpec_v1_genesisHash").await?)?;
        ensure!(
            genesis_hash.len() == 66
                && genesis_hash.starts_with("0x")
                && genesis_hash[2..]
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit()),
            "invalid genesis hash from engine"
        );
        let properties = startup_query(&mut session, "chainSpec_v1_properties").await?;
        let mut methods = startup_query(&mut session, "rpc_methods").await?;
        let mut rpc_methods: Vec<String> = serde_json::from_value(
            methods
                .get_mut("methods")
                .context("engine omitted supported methods")?
                .take(),
        )?;
        rpc_methods.retain(|method| allowed_method(method));
        drop(session);
        entries.push(ChainInfo {
            id: chain.clone(),
            name,
            genesis_hash,
            relay_chain,
            para_id,
            properties,
            rpc_methods,
        });
    }
    Ok(HostInfo {
        protocol_version: PROTOCOL_VERSION,
        read_only: true,
        chains: entries,
        content: Some(crate::content::capabilities(chains)),
    })
}

async fn startup_query(session: &mut Session, method: &str) -> Result<Value> {
    // Requests are sequential on a private frontend; no subscriptions or retries.
    tokio::time::timeout(Duration::from_secs(5), async {
        session.request(json!({"jsonrpc":"2.0","id":1,"method":method,"params":[]}).to_string())?;
        let response = session
            .responses
            .next()
            .await
            .context("startup RPC stream closed")?;
        ensure!(
            response.len() <= MAX_RESPONSE_BYTES,
            "startup RPC response exceeds limit"
        );
        let mut response: Value = serde_json::from_str(&response)?;
        ensure!(
            response.get("jsonrpc").and_then(Value::as_str) == Some("2.0")
                && response.get("id") == Some(&json!(1))
                && response.get("error").is_none(),
            "invalid startup RPC response for {method}"
        );
        response
            .as_object_mut()
            .and_then(|response| response.remove("result"))
            .context("startup RPC response omitted result")
    })
    .await
    .with_context(|| format!("startup RPC {method} exceeded five seconds"))?
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ParachainMetadata<'a> {
    #[serde(borrow, alias = "relay_chain")]
    relay_chain: Option<&'a RawValue>,
    #[serde(borrow, alias = "para_id")]
    para_id: Option<&'a RawValue>,
}

fn parse_specification(
    specification: &str,
    parachain: bool,
) -> Result<smoldot::chain_spec::ChainSpec> {
    // Inspect both fields explicitly: relay_chain() alone cannot distinguish
    // absent metadata from an incomplete pair. Keep upstream's field aliases.
    let metadata: ParachainMetadata<'_> =
        serde_json::from_str(specification).context("parsing chain metadata")?;
    if parachain {
        ensure!(
            metadata.relay_chain.is_some() && metadata.para_id.is_some(),
            "parachain specification requires both relay_chain and para_id"
        );
    } else {
        ensure!(
            metadata.relay_chain.is_none() && metadata.para_id.is_none(),
            "relay specification must not contain parachain metadata"
        );
    }
    let spec = smoldot::chain_spec::ChainSpec::from_json_bytes(specification)
        .context("parsing chain specification")?;
    ensure!(
        !spec.id().is_empty()
            && spec
                .id()
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
        "chain id must contain only ASCII letters, digits, hyphens, or underscores"
    );
    Ok(spec)
}

fn validate_specifications(
    specification: &str,
    parachain_specifications: &[String],
) -> Result<(Vec<String>, usize)> {
    let relay = parse_specification(specification, false).context("invalid relay specification")?;
    let block_number_bytes = usize::from(relay.block_number_bytes());
    let mut chains = Vec::with_capacity(1 + parachain_specifications.len());
    chains.push(relay.id().to_owned());
    let mut para_ids = HashSet::with_capacity(parachain_specifications.len());
    for specification in parachain_specifications {
        let spec =
            parse_specification(specification, true).context("invalid parachain specification")?;
        let (parent, para_id) = spec
            .relay_chain()
            .context("parachain specification lacks complete parent metadata")?;
        ensure!(
            parent == relay.id(),
            "parachain {} requires relay {parent}, not {}",
            spec.id(),
            relay.id()
        );
        ensure!(
            !chains.iter().any(|chain| chain == spec.id()),
            "duplicate chain route id: {}",
            spec.id()
        );
        ensure!(
            para_ids.insert(para_id),
            "duplicate parachain id: {para_id}"
        );
        chains.push(spec.id().to_owned());
    }
    Ok((chains, block_number_bytes))
}

fn config<'a>(
    specification: &'a str,
    database_content: &'a str,
    json_rpc: AddChainConfigJsonRpc,
    relay_anchor: Option<ChainId>,
) -> AddChainConfig<'a, (), std::option::IntoIter<ChainId>> {
    AddChainConfig {
        specification,
        json_rpc,
        database_content,
        potential_relay_chains: relay_anchor.into_iter(),
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
            // Drain that response before enqueueing another: we enforce one
            // outstanding request here, independently of upstream's advisory limit.
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
    application: bool,
    pub responses: JsonRpcResponses<Platform>,
}

impl Session {
    pub fn open(engine: SharedEngine, chain: &str) -> Result<Self> {
        Self::open_inner(engine, chain, true)
    }

    fn open_inner(engine: SharedEngine, chain: &str, application: bool) -> Result<Self> {
        let mut locked = engine.lock().expect("engine lock poisoned");
        let Engine {
            client,
            chains,
            active_sessions,
        } = &mut *locked;
        let configured = chains.get(chain).context("unknown chain route")?;
        // Smoldot deduplicates chain services but creates a fresh RPC frontend.
        let added = client
            .add_chain(config(
                &configured.specification,
                "",
                // Upstream 4.0.0 ignores these settings. Public RPC admission is
                // enforced in ipc; private startup/content callers are sequential.
                AddChainConfigJsonRpc::Enabled {
                    max_pending_requests: NonZeroU32::new(if application {
                        MAX_PENDING_REQUESTS
                    } else {
                        1
                    })
                    .unwrap(),
                    max_subscriptions: if application { MAX_SUBSCRIPTIONS } else { 0 },
                },
                configured.relay_anchor,
            ))
            .map_err(|error| anyhow::anyhow!("adding session: {error}"))?;
        let responses = match added.json_rpc_responses {
            Some(responses) => responses,
            None => {
                let () = client.remove_chain(added.chain_id);
                anyhow::bail!("missing RPC response stream");
            }
        };
        if application {
            *active_sessions += 1;
            log::info!("host: session opened active={active_sessions} chain={chain}");
        }
        drop(locked);
        Ok(Self {
            engine,
            chain_id: added.chain_id,
            application,
            responses,
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
        if self.application {
            engine.active_sessions -= 1;
            log::info!("host: session closed active={}", engine.active_sessions);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{validate_snapshot, validate_specifications};
    use smoldot::{
        chain::chain_information::{
            ChainInformation, ChainInformationConsensus, ChainInformationFinality,
            ValidChainInformation,
        },
        database::finalized_serialize,
        header::{DigestItemRef, DigestRef, GrandpaConsensusLogRef, Header, HeaderRef},
    };

    // These fixtures exercise configuration only, without a client or network.
    fn specification(id: &str, metadata: serde_json::Value) -> String {
        let mut spec = serde_json::json!({
            "name": "Configuration test",
            "id": id,
            "bootNodes": [],
            "genesis": {"raw": {"top": {}, "childrenDefault": {}}}
        });
        spec.as_object_mut()
            .unwrap()
            .extend(metadata.as_object().unwrap().clone());
        spec.to_string()
    }

    #[test]
    fn accepts_relay_only_and_explicit_parachains() {
        let relay = specification("relay", serde_json::json!({"blockNumberBytes": 8}));
        assert_eq!(
            validate_specifications(&relay, &[]).unwrap(),
            (vec!["relay".to_owned()], 8)
        );
        let parachains = [
            specification(
                "asset-hub",
                serde_json::json!({"relay_chain": "relay", "para_id": 1000}),
            ),
            specification(
                "other_chain",
                serde_json::json!({"relayChain": "relay", "paraId": 1001}),
            ),
        ];
        let (routes, width) = validate_specifications(&relay, &parachains).unwrap();
        assert_eq!(routes, ["relay", "asset-hub", "other_chain"]);
        assert_eq!(width, 8);
    }

    #[test]
    fn rejects_missing_incomplete_and_wrong_parent_metadata() {
        let relay = specification("relay", serde_json::json!({}));
        for metadata in [
            serde_json::json!({}),
            serde_json::json!({"relay_chain": "relay"}),
            serde_json::json!({"para_id": 1000}),
            serde_json::json!({"relay_chain": "relay", "para_id": null}),
            serde_json::json!({"relay_chain": null, "para_id": 1000}),
            serde_json::json!({"relay_chain": null, "para_id": null}),
            serde_json::json!({"relayChain": "relay"}),
            serde_json::json!({"paraId": 1000}),
            serde_json::json!({"relay_chain": "wrong-relay", "para_id": 1000}),
            serde_json::json!({"relay_chain": "", "para_id": 1000}),
            serde_json::json!({"relay_chain": "relay", "para_id": -1}),
            serde_json::json!({"relay_chain": "relay", "para_id": "1000"}),
            serde_json::json!({"relay_chain": "relay", "para_id": 4294967296u64}),
        ] {
            let parachain = specification("asset-hub", metadata.clone());
            assert!(
                validate_specifications(&relay, &[parachain]).is_err(),
                "accepted invalid metadata: {metadata}"
            );
        }
        // A perfectly valid relay spec still cannot be installed as a parachain.
        assert!(validate_specifications(&relay, &[relay.clone()]).is_err());
    }

    #[test]
    fn rejects_parachain_metadata_on_relay_input() {
        for metadata in [
            serde_json::json!({"relay_chain": "parent", "para_id": 1000}),
            serde_json::json!({"relay_chain": "parent"}),
            serde_json::json!({"para_id": 1000}),
            serde_json::json!({"relayChain": "parent", "paraId": 1000}),
        ] {
            let relay = specification("relay", metadata);
            assert!(validate_specifications(&relay, &[]).is_err());
        }
    }

    #[test]
    fn rejects_duplicate_routes_and_parachain_ids() {
        let relay = specification("relay", serde_json::json!({}));
        let para = |id, para_id| {
            specification(
                id,
                serde_json::json!({"relay_chain": "relay", "para_id": para_id}),
            )
        };
        for parachains in [
            vec![para("relay", 1000)],
            vec![para("asset-hub", 1000), para("asset-hub", 1001)],
            vec![para("asset-hub", 1000), para("alias", 1000)],
        ] {
            assert!(validate_specifications(&relay, &parachains).is_err());
        }
    }

    #[test]
    fn rejects_unsafe_route_ids_for_every_chain() {
        let relay = specification("relay", serde_json::json!({}));
        for id in ["", ".", "..", "a/b", "a?b", "a#b", "a%2Fb", "a b", "páséo"] {
            let invalid_relay = specification(id, serde_json::json!({}));
            assert!(validate_specifications(&invalid_relay, &[]).is_err());
            let invalid_para = specification(
                id,
                serde_json::json!({"relay_chain": "relay", "para_id": 1000}),
            );
            assert!(validate_specifications(&relay, &[invalid_para]).is_err());
        }
    }

    #[test]
    fn rejects_invalid_upstream_specs_and_duplicate_metadata_fields() {
        let relay = specification("relay", serde_json::json!({}));
        assert!(validate_specifications("not JSON", &[]).is_err());
        assert!(validate_specifications(r#"{"id":"relay"}"#, &[]).is_err());
        assert!(validate_specifications(&relay, &["{}".to_owned()]).is_err());
        let ambiguous = specification(
            "asset-hub",
            serde_json::json!({
                "relay_chain": "relay",
                "relayChain": "different-parent",
                "para_id": 1000
            }),
        );
        assert!(validate_specifications(&relay, &[ambiguous]).is_err());
    }

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
