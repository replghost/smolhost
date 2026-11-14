// SPDX-License-Identifier: GPL-3.0-or-later

use std::{
    num::NonZeroU32,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, ensure};
use smoldot_light::{AddChainConfig, AddChainConfigJsonRpc, ChainId, Client, JsonRpcResponses};

use crate::platform::Platform;
pub type SharedEngine = Arc<Mutex<Engine>>;

pub struct Engine {
    client: Client<Platform>,
    specification: String,
    active_sessions: usize,
}

impl Engine {
    pub fn new(specification: String) -> Result<(SharedEngine, String)> {
        let spec: serde_json::Value = serde_json::from_str(&specification)?;
        let chain = spec["id"]
            .as_str()
            .context("chain spec is missing id")?
            .to_owned();
        ensure!(
            !chain.is_empty()
                && chain
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "chain id must contain only ASCII letters, digits, hyphens, or underscores"
        );
        let mut client = Client::new(Platform::new());
        // This RPC-free anchor keeps sync alive between application connections.
        client
            .add_chain(config(&specification, AddChainConfigJsonRpc::Disabled))
            .map_err(|error| anyhow::anyhow!("adding chain: {error}"))?;
        log::info!("host: engine ready chain={chain}");
        Ok((
            Arc::new(Mutex::new(Self {
                client,
                specification,
                active_sessions: 0,
            })),
            chain,
        ))
    }
}

fn config(
    specification: &str,
    json_rpc: AddChainConfigJsonRpc,
) -> AddChainConfig<'_, (), std::iter::Empty<ChainId>> {
    AddChainConfig {
        specification,
        json_rpc,
        database_content: "",
        potential_relay_chains: std::iter::empty(),
        user_data: (),
        statement_protocol_config: None,
        max_pending_transactions: NonZeroU32::new(1).unwrap(),
    }
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
