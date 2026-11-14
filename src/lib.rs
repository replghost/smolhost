// SPDX-License-Identifier: GPL-3.0-or-later

use std::path::PathBuf;

use anyhow::{Context, Result, ensure};

pub mod client;
pub mod content;
pub mod discovery;
pub mod engine;
pub mod ipc;
mod platform;
pub mod state;

/// Resolve the per-user endpoint without falling back to a shared temporary path.
pub fn default_socket_path() -> Result<PathBuf> {
    let runtime = PathBuf::from(
        std::env::var_os("XDG_RUNTIME_DIR").context("XDG_RUNTIME_DIR is unset; pass --socket")?,
    );
    ensure!(runtime.is_absolute(), "XDG_RUNTIME_DIR must be absolute");
    Ok(runtime.join("smolhost/rpc.sock"))
}
