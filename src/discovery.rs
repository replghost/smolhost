// SPDX-License-Identifier: GPL-3.0-or-later

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Version of the host discovery protocol, independent of chain/runtime versions.
pub const PROTOCOL_VERSION: u32 = 1;
pub const DISCOVERY_PATH: &str = "/host";
pub const DISCOVERY_METHOD: &str = "host_discover";

/// Static configuration and transport capabilities, not synchronization readiness.
#[derive(Debug, Serialize, Deserialize)]
pub struct HostInfo {
    pub protocol_version: u32,
    pub read_only: bool,
    pub chains: Vec<ChainInfo>,
}

/// A configured chain. Route IDs are local names; applications should pin genesis.
#[derive(Debug, Serialize, Deserialize)]
pub struct ChainInfo {
    pub id: String,
    pub name: String,
    pub genesis_hash: String,
    pub relay_chain: Option<String>,
    pub para_id: Option<u32>,
    pub properties: Value,
    pub rpc_methods: Vec<String>,
}
