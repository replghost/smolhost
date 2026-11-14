# polkadot-host

A shared Polkadot light-client service for Linux applications.

**Experimental, unofficial, and at the repository-bootstrap stage. There is no
runnable daemon yet.** This repository currently contains the design, spike
acceptance criteria, license, and an empty Rust workspace. It is not affiliated
with or endorsed by Parity Technologies or the Web3 Foundation.

## Why

Independent applications should be able to use one local light-client service
rather than each starting its own networking and synchronization. The initial
targets are Omarchy desktop and Omarchy Mobile, using standard Linux interfaces
rather than an Omarchy-specific engine.

The service will use [smoldot-light](https://github.com/paritytech/smoldot) as a
library, not fork or reimplement the blockchain client.

## Proposed design

- `polkadot-hostd`: an unprivileged, per-user daemon embedding native smoldot.
- `polkadot-host`: a diagnostic and control CLI.
- One shared engine, with connection-local JSON-RPC sessions and bounded queues.
- Standard [Polkadot JSON-RPC](https://github.com/paritytech/json-rpc-interface-spec)
  over WebSocket framing on a Unix-domain socket.
- Endpoint: `$XDG_RUNTIME_DIR/polkadot-host/rpc.sock`; chain selection through
  `/rpc/<configured-chain-id>` during the WebSocket handshake.
- A systemd user service and socket unit, with one daemon for all connections.
- Curated chain specifications with explicit relay-chain relationships.
- Persisted bootstrap state and different desktop/mobile idle policies.

Native applications will need Unix-socket transport support. Browser pages,
including local `file://` pages, cannot directly access this endpoint. A browser
bridge or authenticated loopback WebSocket adapter is outside the first spike.

The initial implementation should remain one Rust package with two binaries and
modules for the engine, sessions, IPC, and lifecycle. The workspace is empty until
those components have real implementations; it contains no placeholder programs.
The declared `smoldot-light = "=4.0.0"` workspace dependency records the starting
version, but has not yet been built or validated on either target architecture.

## First spike: acceptance criteria

These are planned checks, not claims of implemented behavior.

- [ ] Connect to one public testnet with native smoldot and report finalized blocks.
- [ ] Have two independent client processes follow that chain through one daemon,
      with evidence that the underlying chain synchronization is shared.
- [ ] Read actual chain storage through the local RPC endpoint.
- [ ] Disconnect one client without interrupting the other; release its RPC state.
- [ ] Bound a stalled client's output queue without silently dropping ordered chain
      events or blocking another client indefinitely.
- [ ] Restart the daemon, recover persisted bootstrap state, and reconnect clients
      with new subscriptions rather than pretending old sessions survived.
- [ ] Add a public testnet parachain and verify its relay-chain dependency and sharing.
- [ ] Exercise systemd socket activation and reject connections from other UIDs.
- [ ] Run on x86-64 and AArch64; record memory use, startup behavior, and phone
      suspend/resume behavior on actual hardware.

## Security and scope

- No wallet keys, seed phrases, or signing. Transactions must be signed elsewhere.
- No validator, collator, archive node, indexer, or arbitrary application execution.
- No Zenoh, remote discovery, or cross-device routing in the initial scope.
- No silent fallback from local verification to a trusted remote RPC provider.
- Finalized state is the correctness boundary; best-block state is provisional.
- Use a private runtime directory, restrictive socket permissions, and peer UID
  checks. These protect between users, not between hostile apps sharing one UID.
- Enforce session and global limits, including chains, requests, subscriptions,
  pinned blocks, and queued output bytes.
- Do not promise complete history or compatibility with every full-node RPC method.
- A suspended phone cannot maintain live network synchronization. Resume must
  reconnect and expose stale/offline state honestly.

This project has not been security-audited. Do not use it for production funds.

## Development

The bootstrap manifest can be inspected without downloading dependencies:

```sh
cargo metadata --offline --no-deps --format-version 1
```

There are no buildable workspace members, installation instructions, or service
units yet. Those belong with the working spike, not with a nonfunctional skeleton.
Commit the resolved Cargo lockfile when the first package is added.

Design discussion and reproducible findings are welcome in
[GitHub issues](https://github.com/replghost/polkadot-host/issues). Include the
platform and dependency versions; never include secrets or private chain data.

## License

Original project material is licensed under the GNU General Public License,
version 3 or (at your option) any later version: **GPL-3.0-or-later**. See [LICENSE](LICENSE).

The intended `smoldot-light` 4.0.0 dependency declares
`GPL-3.0-or-later WITH Classpath-exception-2.0` in its
[published manifest](https://docs.rs/crate/smoldot-light/4.0.0/source/Cargo.toml).
Its exception applies to upstream material; this project does not grant that
exception for its own code. Dependencies retain their respective licenses and
notices. Review the complete resolved dependency graph before distributing binaries.
