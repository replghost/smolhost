// SPDX-License-Identifier: GPL-3.0-or-later

use anyhow::{Context, Result, ensure};
use frame_metadata::RuntimeMetadataPrefixed;
use parity_scale_codec::Decode;

/// Metadata_metadata returns SCALE OpaqueMetadata, not bare runtime metadata.
pub fn decode(output: &str) -> Result<RuntimeMetadataPrefixed> {
    let hex = output.strip_prefix("0x").context("expected 0x metadata")?;
    ensure!(
        hex.len() <= 8 * 1024 * 1024,
        "runtime metadata exceeds 4 MiB"
    );
    let encoded = hex::decode(hex).context("invalid metadata hex")?;
    let mut input = encoded.as_slice();
    let opaque = Vec::<u8>::decode(&mut input).context("invalid Metadata_metadata output")?;
    ensure!(
        input.is_empty(),
        "trailing bytes after runtime metadata output"
    );
    let mut input = opaque.as_slice();
    let metadata =
        RuntimeMetadataPrefixed::decode(&mut input).context("invalid runtime metadata")?;
    ensure!(input.is_empty(), "trailing bytes after runtime metadata");
    ensure!(
        metadata.0 == frame_metadata::META_RESERVED,
        "invalid metadata magic"
    );
    Ok(metadata)
}
