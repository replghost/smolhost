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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<ContentCapabilities>,
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

/// Static content transport support, not availability or chain provenance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentCapabilities {
    pub protocol_version: u32,
    pub endpoints: Vec<ContentEndpoint>,
    pub cid_versions: Vec<u64>,
    pub multibases: Vec<String>,
    pub codecs: Vec<u64>,
    pub hashes: Vec<u64>,
    pub unixfs_types: Vec<String>,
    pub limits: ContentLimits,
    pub deadlines: ContentDeadlines,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentEndpoint {
    pub chain_id: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentLimits {
    pub max_file_bytes: u64,
    pub max_block_bytes: u64,
    pub max_blocks: u64,
    pub max_depth: u64,
    pub max_dag_overhead_bytes: u64,
    pub max_chunk_bytes: u64,
    pub max_control_bytes: u64,
    pub max_concurrent_transfers: u64,
    pub max_connections: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentDeadlines {
    pub handshake_ms: u64,
    pub request_ms: u64,
    pub write_ms: u64,
    pub transfer_ms: u64,
}
