# Smolhost

A shared Polkadot light-client service for Linux applications, powered by smoldot.

**Experimental, unofficial prototype.** Not security-audited or suitable
for production funds. Independent of the smoldot maintainers, Parity Technologies
and the Web3 Foundation.

## What this spike does

- Runs native [smoldot-light](https://github.com/paritytech/smoldot) in one daemon.
- Exposes read-only Polkadot JSON-RPC over WebSocket framing on a Unix socket.
- Gives each connection its own RPC session while sharing chain synchronization.
- Includes a CLI that follows finalized blocks and reads `System.Number` from
  verified chain storage, plus an opt-in two-process acceptance script.
- Loads pinned Paseo relay and Next v2 Asset Hub, Bulletin and Individuality specs
  in one client, sharing the relay across all three application chains.
  No wallet or credentials are needed, and no transactions are submitted.
- Can run as an opt-in systemd user service, with a supervisor-owned Unix socket.
- Saves relay bootstrap checkpoints every 60 seconds and on clean shutdown.
- Discovers configured chains and read-only RPC capabilities over `/host`.
- Provides a reusable native Rust client; the CLI uses the same adapter.
- Includes a separate native-balance application example using the public client.
- Streams CID-verified Bulletin files over a separate content route, with a native
  download client that publishes only complete files without overwriting paths.
- Provides an opt-in endurance harness for finalized-read stability and Linux
  process resource measurements.

There is no browser bridge, TCP listener, signing, or Zenoh integration. Desktop
and mobile are the intended targets; an x86-64 run does not establish AArch64 or
phone support.

## Chain defaults and application needs

The installed service enables **Paseo Next v2's application profile**: Asset Hub,
Bulletin and Individuality/People, with one shared Paseo relay dependency. It does
not enable production Polkadot chains or mix in Products Devnet. Classic Paseo
Asset Hub remains an explicit manual/baseline fixture, not the installed default.

The profile is deliberately smaller than the set of all system chains:

| Role | What applications need it for | Policy |
| --- | --- | --- |
| Relay chain | Parachain verification and finality | Shared dependency |
| Asset Hub | Assets, balances, runtime/contract reads, naming where deployed | Enabled: `next-asset-hub-paseo`, para 1500 |
| Bulletin | Content-addressed files, app bundles, media | Enabled RPC and native Bitswap file retrieval: `next-bulletin-paseo`, para 1501 |
| Individuality / People | Identity, personhood and membership policy | Enabled RPC: `next-people-paseo`, para 1502; not interchangeable with other People deployments |
| Bridge Hub, Collectives, Coretime | Bridges, collectives/fellowship, coretime operations | Explicitly configure only when an application needs them |

The public [Product SDK chain client](https://docs.polkadot.com/apps/product-sdk/chain-client/)
uses these three application-facing roles. We pin its **Paseo Next v2** deployment,
not an environment inferred from friendly names. Its [network presets](https://docs.polkadot.com/apps/concepts/networks/)
distinguish this from Products Devnet and classic Asset Hub. The compact fixtures'
computed genesis hashes match independently pinned SDK descriptors; see
[`chains/paseo-next-v2-provenance.json`](chains/paseo-next-v2-provenance.json).
This daemon is not a Product SDK Host provider.

[Individuality](https://github.com/paritytech/individuality-community) describes
itself as experimental People-chain infrastructure, not part of the official
People chain. [Bulletin](https://github.com/paritytech/polkadot-bulletin-chain) is a
storage parachain with authorization and retention rules; ordinary storage RPC
does not by itself deliver its content-retrieval service.

Beyond chain reads, applications may need:

- Verified-runtime metadata/typed bindings, app-specific capability checks and
  synchronization readiness. Configured-chain discovery and native transport are
  now available below.
- Wallet-mediated signing and explicit transaction approval/submission. Keys
  should stay outside this read-only daemon.
- Optional application-owned content caching and Bulletin retention/renewal policy;
  signed-statement gossip, allowances and expiry handling for Statement Store.
- Network-specific contract/name-service configuration, and optional indexers for
  history/search that a light client does not provide.
- Reconnection/resubscription and an idle/suspend policy appropriate for phones.

Beyond discovery, native transport and read-only content retrieval, these are
recommendations, not implemented host APIs.
Currently all explicitly configured chains stay active until the daemon stops.
Per-chain on-demand activation and idle unloading are not implemented; measure
their resource/battery tradeoffs before expanding the always-running defaults.
None of the above requires introducing Zenoh.

## Build

A Linux environment and a Rust toolchain supporting edition 2024 are required.
Python 3 is needed for the installer and acceptance script. The lockfile fixes
dependency versions, including `smoldot-light` 4.0.0 and its native runtime dependencies.

```sh
cargo build --locked --bins
```

Build artifacts go in `target/` unless your local build tooling overrides that
location. Source and chain state must not be stored in disposable build outputs.

## Run manually

Start the daemon in one terminal:

```sh
./target/debug/smolhostd --chain-spec chains/paseo.json \
  --parachain-spec chains/paseo_next_asset_hub.json \
  --parachain-spec chains/paseo_next_bulletin.json \
  --parachain-spec chains/paseo_next_individuality.json
```

It creates `$XDG_RUNTIME_DIR/smolhost/rpc.sock` and connects to public Paseo
peers. Synchronization may take time; readiness of the socket does not mean the
chain is synchronized. No remote RPC fallback is used.

`--chain-spec` selects one relay. Repeat `--parachain-spec PATH` for additional
parachains on that relay; omit it for relay-only operation. Specifications must
have unique safe route IDs and unique parachain IDs, with matching relay metadata.
Invalid or ambiguous configurations fail startup. Applications cannot register
arbitrary chains through RPC.

The bundled bootnodes include secure WebSockets. A small native platform adapter adds
WSS using Rustls and Mozilla trust roots from `webpki-roots`, with hostname and
certificate verification enabled. Smoldot's default native platform handles the
executor, clocks, randomness, and non-TLS connections. TLS does not replace
smoldot's peer authentication, finality verification, or storage-proof checking.

List configured chains without selecting one:

```sh
./target/debug/smolhost chains
```

This prints a single JSON object with the protocol version, read-only policy, and
each chain's route ID, name, genesis hash, relay/parachain relationship, properties
and supported read-only RPC methods. It does not wait for chain synchronization.

In another terminal:

```sh
./target/debug/smolhost --chain next-asset-hub-paseo probe --finalizations 3
```

Use `--chain next-bulletin-paseo` or `--chain next-people-paseo` for the other app
chains, or `--chain paseo` for the shared relay. Handshake paths are
`/rpc/<chain-id>`. A connection selects one chain once; its requests and
subscriptions remain isolated from other connections.

The probe prints JSON lines: `initialized`, `storage`, `finalized`, and `completed`.
It reads a four-byte SCALE `System.Number` at the initial finalized block and after
each subsequent finalized notification, verifies increasing numbers, and releases
obsolete block pins. Its default total deadline is 240 seconds; use
`--timeout-secs` to change it. Errors, disconnection, or a stopped subscription
fail the probe rather than being hidden by retries.
The probe first discovers the selected route, then checks that the connected
session's genesis matches its advertised identity.

For raw RPC, the CLI streams responses until disconnect, Ctrl-C, or its deadline:

```sh
./target/debug/smolhost --chain paseo rpc \
  '{"jsonrpc":"2.0","id":1,"method":"chainHead_v1_follow","params":[false]}'
```

Raw mode does not manage pinned blocks for you; use `probe` for the bounded smoke
scenario. Both binaries accept `--socket /absolute/private-directory/rpc.sock`.
The socket directory must be owned by the current user and inaccessible to other
users. An existing socket is never replaced by the manual daemon. Stop it with
Ctrl-C or SIGTERM; it cancels its sessions and removes its own socket. After an
unclean manual-run crash, verify that no daemon owns the endpoint before manually
removing a stale socket. Do not run the service and a manual daemon on the same
path; use `--socket` for an independent experiment.

## Native application client and discovery

The Rust library exposes `smolhost::client::{discover, connect_chain,
Connection, RpcClient}`. Applications can use this repository as a Git/path
dependency; it is not published on crates.io. Pin a reviewed revision. The client
currently lives in the same package as the daemon and uses its dependency graph.

For example, select the default Paseo Next v2 Asset Hub by an independently pinned
genesis rather than trusting a friendly route name:

```rust
use anyhow::{Context, Result};
use smolhost::client::{connect_chain, discover};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let socket = smolhost::default_socket_path()?;
    let host = discover(&socket).await?;
    let expected = "0x4349b00e54897e21196fd331015fc5be0f14e118beb0375ed2bb1793737bb57a";
    let chain = host.chains.iter()
        .find(|chain| chain.genesis_hash == expected)
        .context("required Asset Hub genesis is not configured")?;
    let mut rpc = connect_chain(&socket, chain).await?;
    let properties = rpc.call("chainSpec_v1_properties", json!([])).await?;
    println!("{properties}");
    rpc.close().await?;
    Ok(())
}
```

`RpcClient::call` is sequential; `next_notification` returns notifications in wire
order, including those received while a call awaited its response. The backlog is
bounded to 256 envelopes and 8 MiB of original JSON bytes. Overflow fails rather
than silently discarding events. `Connection` offers raw JSON send/receive for
applications managing their own request/subscription routing.

Connections verify the daemon's UID and have a five-second connect/handshake
deadline. Apply your own overall timeout to RPC operations. Cancelling an
in-flight operation or encountering a transport/protocol error makes the client
unusable: drop it, reconnect and explicitly resubscribe. A valid remote JSON-RPC
error does not poison the session. There are no hidden tasks, retries or reconnects.
Closing sends a close frame but does not wait for peer acknowledgment.

Other native languages can use the same Unix WebSocket protocol. Connect to
`/host` on the existing socket and send:

```json
{"jsonrpc":"2.0","id":1,"method":"host_discover","params":[]}
```

The result is protocol version 1 `HostInfo` from `src/discovery.rs`. `/host` also
supports `rpc_methods`; both methods accept omitted parameters or `[]` only.
Chain RPC stays on `/rpc/<id>`. Discovery does not open application chain sessions,
register chains or expose chain specs/checkpoints. The catalog is collected from
smoldot once during startup and is immutable until restart.

**Configured is not synchronized.** Discovery's method list describes the
transport API, not runtime pallets, contracts, personhood availability or finalized
state readiness. Use chain RPC and real verified reads for those checks. Discovery
is not an independent trust source, a browser bridge or a Product SDK provider.

### Content capability discovery

`HostInfo.content` is an optional `ContentCapabilities` descriptor. Discovery
remains protocol version 1: absence means content retrieval is unavailable, not
permission to guess a route. Older hosts can still serve ordinary chain RPC.
The content protocol has its own `protocol_version`.

- `endpoints` maps each configured `chain_id` to its actual local `path`.
  This includes the relay and does not claim those chains are Bulletin.
- `cid_versions`, `multibases`, `codecs`, `hashes` and `unixfs_types` advertise
  accepted formats. Codec/hash entries are numeric multicodec/multihash codes;
  the current multibase label is `base32lower`.
- `limits` contains `max_file_bytes`, `max_block_bytes`, `max_blocks`, `max_depth`,
  `max_dag_overhead_bytes`, `max_chunk_bytes`, `max_control_bytes`,
  `max_concurrent_transfers` and `max_connections`. Byte fields use bytes;
  connections are the global budget across all routes, not extra content slots.
- `deadlines` contains `handshake_ms`, `request_ms`, `write_ms` and `transfer_ms`.
  Values come from the same constants used for server enforcement.

The complete typed schema is in [`src/discovery.rs`](src/discovery.rs).
`smolhost chains` prints it without requiring chain synchronization.
These capabilities describe support, not peer readiness or content availability.

`content::client::fetch` and `fetch-blob` discover before downloading. They reject
missing/unsupported capabilities, ambiguous endpoints, unsafe paths and incompatible
framing/DAG limits before opening a content session or creating partial output.
The selected path stays on the same authenticated Unix socket; there is no guessed
endpoint, remote fallback or automatic retry. Compatible extra advertised formats
do not prevent using an already-supported format.

The requested byte allowance is reduced to the server's advertised file limit;
stricter count, frame/control and deadline limits also apply. Advertised limits
never relax the client's local safety bounds. Discovery, transfer and file sync
share the caller's total timeout; the advertised transfer deadline can tighten it.
Update the daemon when a new client reports that an older host does not advertise
content retrieval, rather than bypassing discovery.

## Read the application profile

Build and run a separate application against the default/manual Next v2 profile:

```sh
cargo build --locked --release --bins --example read-app-profile
./target/release/examples/read-app-profile
```

It discovers all three routes by independently pinned genesis hashes, then reads
each at a verified finalized block using that block's runtime metadata:

| Role | Read | Meaning |
| --- | --- | --- |
| Asset Hub | `System.Account` | Exact native balance fields for the public Alice fixture, or your `--address` |
| Bulletin | `TransactionStorage.RetentionPeriod` | Data-retention policy in blocks, distinguishing stored value from metadata default |
| Individuality | `Members.OldRootRetentionDuration` | Verified runtime constant: membership-ring old-root grace period in seconds |

Use `--socket` for another private endpoint and `--timeout-secs` for the total
deadline. JSONL includes genesis, block, runtime-code hash, metadata version,
source kind and exact integer strings. These are separate finalized snapshots,
not an atomic cross-chain view. A later chain can fail after earlier records were
printed: check the process exit status. Missing roles and unsupported schemas fail
explicitly, without public-RPC fallback.

These reads do not fetch Bulletin blobs, exchange statements, identify people,
prove a user's personhood, sign or submit transactions.

## Retrieve Bulletin files

The daemon reuses smoldot's native Bitswap service: no IPFS daemon, gateway,
remote RPC fallback, or persistent content cache. Chain synchronization and peer
connections are shared with existing sessions. This is read-only: it cannot
upload, renew, sign, submit transactions, or exchange signed statements.

Build the external consumer, then run it against a daemon with Bulletin configured:

```sh
cargo build --locked --release --example fetch-blob
./target/release/examples/fetch-blob \
  --cid bafybeicms2uw6i5r6z7djjqk2365uh4zlerd6xd4z4iw642huojvtpjzhq \
  --output ./bulletin-file.bin
```

The example defaults to `next-bulletin-paseo` and the normal private socket.
Override `--socket`, `--chain`, `--max-bytes`, or `--timeout-secs` as needed.
The sample CID is public testnet content, not a permanent availability promise;
Bulletin retention and peer availability still apply. There are no hidden
retries. A newly started daemon may need time to connect to Bitswap peers.

Rust applications can call `smolhost::content::client::fetch(socket, chain, cid,
output, FetchOptions)` and receive a `smolhost::content::Receipt`. The daemon
verifies each block's CID and the UnixFS file/subtree sizes before completion.
The client independently checks the streamed byte count and SHA-256 receipt,
syncs a private `0600` temporary file in the destination directory, then atomically
publishes without replacing an existing file or symlink. Failure, timeout, or
Ctrl-C removes the partial file; the daemon never receives the output pathname.
An abrupt process death (including SIGKILL or an unhandled SIGTERM) can leave a
private `.smolhost-content-*` partial file, never a completed destination. Cleanup
on such failures is not automatic. Publication syncs the file, not its parent
directory; survival of the new directory entry across power loss is not guaranteed.

Supported content is canonical lowercase base32 **CIDv1**, raw blocks (`0x55`)
and DAG-PB (`0x70`) UnixFS File/Raw nodes, including inline bytes, nested files
and ordered chunk links. Hashes: SHA2-256, BLAKE2b-256 and Keccak-256. CIDv0,
other encodings/codecs, directories, symlinks and HAMT nodes are rejected.
This is a file reader, not a directory extractor or a general IPFS API.

### Content wire protocol

Connect over the existing private Unix socket to `/content/<configured-chain-id>`.
One connection serves one download. Send a text request:

```json
{"version":1,"cid":"<CID>","max_bytes":67108864}
```

`max_bytes` is optional and defaults to 64 MiB; a caller can only lower it.
Unknown fields and versions are rejected. The server sends:

1. Text `{"event":"started","version":1,"cid":"<CID>","chain":"<chain-id>"}`.
2. Binary file chunks of at most 64 KiB each.
3. Text `{"event":"completed","cid":"<CID>","bytes":N,"blocks":N,"sha256":"<lowercase hex>"}`,
   then closes.

An error sends `{"event":"error","message":"..."}` when the transport permits,
then closes without completion. Previously received chunks remain partial:
never publish or execute them. Send `{"event":"cancel"}` or disconnect to cancel,
including while a block fetch or socket write is pending. The native client
cancels by dropping its connection. Upstream cancellation is best effort for
network requests already sent.

Hard bounds: four simultaneous transfers within the existing 16-connection
budget; one outstanding block fetch per transfer; 64 MiB reconstructed file;
2 MiB per encoded block; 4096 block occurrences including manifests/repeated
references; 32 root-inclusive DAG levels; 4 KiB control messages.
Accepted encoded DAG bytes are limited to `max_bytes + 16 MiB` (at most 80 MiB),
including repeated blocks and metadata. One additional block of up to 2 MiB can
be fetched before its size reveals the excess; it is rejected before hashing or
decoding. This is not a cap on total network protocol overhead.
The request and each blocked write have five-second deadlines; a transfer has
a 180-second deadline. Client deadlines may be shorter. These bounds do not
impose a fixed memory ceiling on smoldot's shared chain/runtime services.

The local daemon is trusted to verify CIDs. Its completion SHA-256 detects
stream corruption but is not an independent proof of the root CID. A CID proves
content identity, **not** chain inclusion, authorship, retention, permission to
execute a bundle, or safety of its contents. The selected chain supplies network
context; Bitswap availability is not chain-provenance evidence.

The adapter uses the pinned smoldot
[`bitswap_unstable_stream` implementation](https://github.com/paritytech/smoldot/blob/6aaa2a8e550f208b3d2182b411c99219680121e0/light-base/src/bitswap_service.rs)
internally; it is not part of the public RPC allowlist. The bounded file walker
supports the format produced by Bulletin's public
[UnixFS DAG builder](https://github.com/paritytech/polkadot-bulletin-chain/blob/ec88c0f52ee4a09213cc0cc105390a190cd41568/sdk/rust/src/dag.rs).

### Content diagnostics

Failures retain the upstream cause and add the chain, a process-local `transfer`
ID, the last observed `phase`, and elapsed milliseconds for the transfer, current
phase and current block. `block` is a one-based traversal occurrence (zero before
the first request). `verified_blocks` and `verified_bytes` describe accepted DAG
progress; `sent_bytes` counts successful local WebSocket writes, not client
receipt or durable publication. Partial progress never establishes a complete file.

The upstream stages distinguish `await_subscription`, `await_block` and
`await_done`. In particular, receiving block bytes does not imply that the
subscription completed. Verification and output stalls have separate stages,
including `verify_block` and `write_chunk`. Cancellation retains the last stage
without claiming completion. Error messages remain bounded human-readable text,
not a machine-stable diagnostic schema.

For per-block start/receive records and completed/cancelled/failed outcomes,
start a diagnostic daemon with `RUST_LOG=info,smolhost::content=debug`, or add
`--content-debug` to the content soak command below. Debug start records include
the requested CID; no block payloads are logged by these records. Treat logs as
private: CIDs can disclose what an application requested, and upstream errors
may contain additional context. Debug logging is off by default.

For upstream peer-selection evidence, add
`bitswap-service-next-bulletin-paseo=trace` to the diagnostic daemon's `RUST_LOG`.
These records include Have responses, block requests, payload-receipt metadata
and peer IDs. Keep them private too. The content harness sets its own log filter;
`--content-debug` alone enables host diagnostics, not upstream trace records.

The pinned [upstream implementation](https://github.com/paritytech/smoldot/blob/6aaa2a8e550f208b3d2182b411c99219680121e0/light-base/src/bitswap_service.rs)
starts a 10-second per-CID timer after a successful Have broadcast. An upstream
`-32811` timeout is distinct from the daemon's transfer deadline and local writer
timeout. `await_block` alone cannot distinguish waiting for a Have response from
waiting for the selected peer's block, or establish the network cause. There are
still no automatic retries or deadline extensions.

## Native balance application

`examples/watch-balance.rs` is a separate application process, not a daemon API
or a wallet. It discovers the pinned classic Paseo Asset Hub and reads native
`System.Account` balances at finalized blocks through the same Unix socket.
It does not watch Assets/ForeignAssets tokens or submit transactions.

Build the example and the daemon:

```sh
cargo build --locked --release --bins --example watch-balance
```

This example deliberately retains the **classic** Asset Hub genesis for baseline
comparisons; it does not target the installed Next v2 profile. Start a separate
classic manual daemon:

```sh
./target/release/smolhostd --chain-spec chains/paseo.json \
  --parachain-spec chains/paseo_asset_hub.json
```

If the installed service is active, stop both units first or give both daemon and
example a separate private `--socket` path. In another terminal:

```sh
./target/release/examples/watch-balance --address YOUR_ACCOUNT --finalizations 3
```

The example uses verified runtime metadata rather than assuming a fixed SCALE
account layout. Balance amounts are exact integer base-unit strings, not
floating-point amounts or an estimate of spendable funds. An absent account is
reported explicitly; it is not evidence of a failed RPC or of missing history.
No signing keys, faucet funding or personal account is required to try it.

`--address` accepts checksum-verified SS58 AccountId32 or `0x` followed by exactly
64 hexadecimal digits. An SS58 network prefix does not select the chain; the
application's genesis pin does. `--socket` selects another daemon endpoint.
The example emits an initial balance and then the requested number of additional
finalized blocks (`--finalizations 0` reads only the initial block). Each JSON
record includes the account ID, block/genesis hashes, existence, metadata version,
runtime-code hash and named balance fields.

Metadata V14–V16 is supported only for the validated `System.Account` schema.
The example verifies the runtime-code hash at every selected block and stops
explicitly on a runtime upgrade instead of decoding with stale metadata.
`--timeout-secs` bounds discovery, synchronization and reads; cleanup can take
up to two additional seconds. Errors and Ctrl-C exit unsuccessfully, without
automatic reconnect/resubscribe. Treat this as a small fail-closed example, not
a production payment monitor.

## Run as a user service

Requires Linux with a running systemd user manager. No root privileges or sudo:

```sh
python3 scripts/user-service.py install
systemctl --user enable --now smolhost.service
~/.local/bin/smolhost --chain next-asset-hub-paseo probe --finalizations 3
```

Build both binaries first. The installer defaults to `target/debug`; use
`--bin-dir /absolute/build/output` for another build. It copies the binaries,
Paseo relay and all three Next v2 specs into `~/.local/lib/smolhost`, links the CLI
in `~/.local/bin`, and copies the units into `$XDG_CONFIG_HOME/systemd/user` (normally
`~/.config/systemd/user`). Nothing runs from disposable build outputs.

Installation alone **does not start or enable anything**. The explicit
`enable --now` command starts the daemon and enables both units for future user
sessions. For this session only, use `systemctl --user start smolhost.service`.
For on-demand activation instead, enable only `smolhost.socket`; the first
connection starts the daemon, which then stays running.

The socket remains `$XDG_RUNTIME_DIR/smolhost/rpc.sock`, mode `0600`, in a
private `0700` directory. Systemd owns the listener and passes it to the daemon.
The daemon validates its path, permissions, and ownership and never unlinks an
inherited endpoint. The socket unit refuses to start over an existing pathname,
including a manual daemon's listener; inspect the owner before removing stale files.

The daemon restarts after failure, subject to a three-starts-per-minute limit.
The socket survives daemon restarts, but **existing application sessions do not**:
clients must reconnect and resubscribe. Startup restores a compatible saved
checkpoint when present, then catches up with peers. An active unit or listening
socket does not establish chain readiness—wait for RPC initialization and real data.

```sh
systemctl --user status smolhost.service smolhost.socket
journalctl --user -u smolhost.service -f
systemctl --user restart smolhost.service
```

To stop it, stop **both** units; leaving the socket running allows connections to
reactivate the daemon:

```sh
systemctl --user stop smolhost.socket smolhost.service
```

To also disable future startup, use `systemctl --user disable --now
smolhost.socket smolhost.service`. This keeps the installation.
For removal, including stopping and disabling both units:

```sh
python3 scripts/user-service.py uninstall
```

The installer refuses conflicting files. Uninstall checks recorded hashes and
refuses modified installed files before stopping anything; back up/remove or
restore those modifications first. Unrelated files and systemd drop-ins are left
alone. For this spike, update by uninstalling and then reinstalling.

The service follows the lifetime of your user manager. This installer does not
enable lingering, change logout policy, or claim operation during device suspend.
See systemd's [socket activation documentation](https://www.freedesktop.org/software/systemd/man/latest/systemd.socket.html).

### Upgrading from the classic two-chain profile

Before updating, uninstall with the old revision's installer. If already updated,
use the [classic-profile installer at `90533dc`](https://github.com/replghost/smolhost/blob/90533dc/scripts/user-service.py)
to remove that receipt-owned installation, then install this revision. The new
installer intentionally does not accept the old file set. Checkpoints are
preserved; the shared relay specification and checkpoint format are unchanged.
Update application genesis pins/routes deliberately: Next v2 Asset Hub is a
different chain, not a route alias for classic Asset Hub.

### Upgrading from polkadot-host

This is a clean rename, not a compatibility alias. Before updating an older
installation, run that revision's `scripts/user-service.py uninstall`; it stops
and removes the old units/binaries while preserving checkpoints. If already
updated, use the [previous installer](https://github.com/replghost/smolhost/blob/414b9f8/scripts/user-service.py)
for that uninstall. Stop any manually launched old daemon as well.

With both daemons stopped, optionally move the entire private state directory
`$XDG_STATE_HOME/polkadot-host` to `$XDG_STATE_HOME/smolhost` (under
`~/.local/state` when XDG_STATE_HOME is unset). Move it only when the destination
does not exist; do not merge, overwrite or remove lock files individually. The
checkpoint format and chain-spec fingerprints are unchanged.

Then build and install Smolhost using the commands above. Update application
dependencies/imports to `smolhost`, commands to `smolhost`/`smolhostd`, and any
explicit socket paths to the new location. Discovery protocol version 1 and
`/host`, `/rpc/<id>` routes are unchanged. The new installer does not manage an
old-name installation, and the daemon never silently imports old state.

## Bootstrap checkpoints

Persistence is on by default, including manual runs. State lives in
`$XDG_STATE_HOME/smolhost` (normally `~/.local/state/smolhost`), outside
the repository and build outputs. Use `--state-dir /absolute/private/directory`
to select another location, or `--no-state` to neither read nor write checkpoints.
These options are mutually exclusive. The user service uses systemd's persistent
`StateDirectory` at the same default location.

The relay's exact chain-spec fingerprint has a JSON checkpoint and a separate
lock file. Different relay spec bytes select a different checkpoint; old files
are not deleted. Only one daemon can own that relay state at a time, even when its
IPC socket differs. Directories must be user-owned `0700`; state files must be
user-owned `0600`. Symlink and nonregular-file endpoints are rejected.

Only relay bootstrap state is persisted. After a restart, parachains derive
verified progress from the restored/shared relay and fetch their own runtime/state
as needed. No independent parachain finality checkpoint or application session
is persisted.

The daemon requests smoldot's `chainHead_unstable_finalizedDatabase` through a
**private** anchor RPC frontend, not through the application socket. It validates
the exported finalized-chain information and writes a versioned envelope with
the pinned engine version, spec fingerprint, and SHA-256 payload checksum.
The smoldot payload is limited to 8 MiB. A same-directory temporary file, file
fsync, atomic rename, and directory fsync prevent in-place partial publication.
Lock files remain on disk; do not remove them while a daemon is running.

Missing state means a normal cold start. Corrupt, incompatible, oversized, or
insecure state fails startup without overwriting it. Periodic export/write failures
are logged and do not stop RPC service; a shutdown-save failure produces a nonzero
daemon exit. Export requests have a five-second deadline, and shutdown waits at
most 12 seconds for the checkpoint worker. Unclean termination can lose progress
since the last completed checkpoint. A failed directory fsync can leave a newly
published checkpoint whose durability is uncertain.

This is a bootstrap cache, **not** a full chain database: it can include finalized
chain information, discovered peers, and runtime hints, not historical storage or
application subscriptions. It originates from locally verified synchronization,
but smoldot subsequently treats saved finality as a **trusted local checkpoint**.
Checksums detect corruption, not malicious same-user modification. Never import
untrusted checkpoint files or publish local state dumps.

Uninstall preserves state. To reset an incompatible or damaged checkpoint, stop
both service units and preserve/move the affected JSON file before restarting;
do not delete the lock file. `--no-state` is also available for an isolated
diagnostic run. There is no silent state reset or remote-RPC fallback.

Smoldot's [database input contract](https://docs.rs/smoldot-light/4.0.0/smoldot_light/struct.AddChainConfig.html#structfield.database_content)
reserves the right to change its format. Engine upgrades need an explicit
checkpoint compatibility decision; this version only loads its pinned format.

## Verify

Offline checks cover client cancellation/notification ordering, discovery routing,
chain/parent configuration, read-only filtering, socket lifecycle, checkpoints,
CID/UnixFS integrity and bounds, content cancellation/backpressure and verified
no-clobber file publication:

```sh
cargo test --locked --lib
```

Focused offline checks for metadata/address decoding and the endurance harness:

```sh
cargo test --locked --release --example watch-balance --example read-app-profile
python3 -B -m unittest discover -s scripts -p 'test_soak.py'
```

For a live test against public Paseo peers, build both binaries first, then run:

```sh
python3 scripts/smoke.py --chain-spec chains/paseo.json \
  --parachain-spec chains/paseo_next_asset_hub.json \
  --parachain-spec chains/paseo_next_bulletin.json \
  --parachain-spec chains/paseo_next_individuality.json --chain next-asset-hub-paseo
```

For the relay-only scenario, omit `--parachain-spec` and use `--chain paseo`.

The script uses its own temporary directory and daemon. It requires:

1. Two separate CLI processes connected at the same time, both reading real state.
2. Client A completing and exiting while client B continues to read later finalized
   state and then completes successfully.
3. Both sessions being released without stopping the daemon.
4. Exactly one relay initialization and one initialization per configured parachain.

The script uses `--no-state`, so it never touches your persistent checkpoints.
It prints a JSON result and stops its child processes on success, failure, or
interruption. It does not install anything, change a live service, or submit a
transaction. Use `--bin-dir` for a custom build output and `--timeout-secs` for a
longer overall deadline. Network availability is required; this is not an offline
unit test.

### Longer-running stability and resource smoke

Build release binaries first; debug-build CPU measurements are not a useful
production baseline:

```sh
cargo build --locked --release --bins
python3 scripts/soak.py --bin-dir target/release \
  --parachain-spec chains/paseo_next_asset_hub.json \
  --parachain-spec chains/paseo_next_bulletin.json \
  --parachain-spec chains/paseo_next_individuality.json \
  --chain next-asset-hub-paseo \
  --expected-genesis 0x4349b00e54897e21196fd331015fc5be0f14e118beb0375ed2bb1793737bb57a \
  --chain next-bulletin-paseo \
  --expected-genesis 0x8cfe6717dc4becfda2e13c488a1e2061ff2dfee96e7d031157f72d36716c0a22 \
  --chain next-people-paseo \
  --expected-genesis 0x4a2b5b737de1da59e209b0000a876ec2fa20035dc34fd292a848da32d255ad48 \
  --duration-secs 21600 --max-rss-mib 4096 \
  --output-dir "$HOME/.local/state/smolhost-soak-$(date -u +%Y%m%dT%H%M%SZ)"
```

This is an opt-in six-hour experiment, not a service installation. The command
above starts its own relay plus three-parachain daemon, a private socket and fresh
private checkpoint state. One continuous client per selected chain reads finalized
`System.Number`; an additional short-lived client rotates among the chains.
Stalls, unexpected exits and invalid reads fail rather than being hidden by
reconnection. Each chain has an independent progress deadline, so one healthy
chain cannot mask another stalling.

Pair each repeated `--chain` with one `--expected-genesis`, in the same order.
There are at most seven monitored chains, leaving capacity for discovery and
churn within the daemon's unchanged connection limit. Without chain/spec overrides
the harness deliberately retains the classic two-chain baseline. For a short
comparison use `--duration-secs 180 --sample-secs 5` for both profiles.

The workload duration starts only after **every monitored chain** has a verified
initial read; startup has its own deadline. Defaults sample every 10 seconds,
churn a client every 60 seconds, and fail after 240 seconds without advancing
finalized reads on any monitored chain. `SOAK_READY` is not a completed test.

The output directory must not already exist. Keep it outside disposable build
outputs and do not publish it wholesale: it contains local logs and trusted
checkpoint state. Review the machine-readable summary and samples instead.
The run stops its own children at the deadline or on interruption; it never
stops an installed Smolhost service. An interrupted run is not a pass.
Evidence files are `metadata.json`, `samples.jsonl`, `progress.jsonl`, bounded
role logs and `summary.json`. The summary separates startup, active workload and
steady measurements (excluding the first 60 workload seconds), plus per-chain
read counts/finalized progress, checkpoint shutdown and process-exit evidence.
Missing summary after power loss
or SIGKILL is not a pass. Use immutable binaries for a run so later rebuilds do
not change its periodically launched clients.

Short-lived churn clients may close their pipes just before their process exit is
observable. After verified protocol completion, the harness allows at most five
seconds for a zero exit; EOF alone never counts as success. Missing completion,
nonzero exits, hangs and closed output from continuous processes still fail.

Measurements cover the daemon process: resident memory (RSS), CPU time, threads
and open file descriptors. CPU `100%` means one full logical core, not the whole
machine. Compare cold-start and established-sync periods separately. A rising
RSS sample series is a signal to investigate, not proof of a leak; stable RSS
over six hours is not proof that leaks are impossible. The explicit 4 GiB RSS
cap above is an unattended-run safety limit, not an expected footprint.

This workload measures an active shared light client, not an idle/on-demand host,
complete application resource use, mobile battery drain or suspend recovery.

### Content retrieval soak

`scripts/content-soak.py` tests the content endpoint independently of the chain-read
soak. It launches its own no-state daemon with the default application profile,
waits for a verified finalized Bulletin read, then exercises concurrent download
pairs, cancellation after real bytes, an actual nonreading WebSocket consumer,
and successful recovery downloads. It never connects to an existing service.

```sh
cargo build --locked --release --bins --example fetch-blob
python3 -B scripts/content-soak.py --bin-dir target/release \
  --duration-secs 900 \
  --output-dir "$HOME/.local/state/smolhost-content-soak-$(date -u +%Y%m%dT%H%M%SZ)" \
  --cid bafybeicms2uw6i5r6z7djjqk2365uh4zlerd6xd4z4iw642huojvtpjzhq \
  --expected-sha256 91d59231ddad7d2e94c313aa5d9b95cc44b2a4775fdaaf24f6cd3f17f2cf1155 \
  --expected-bytes 10493449 --expected-blocks 10 \
  --churn-cid bafkreifmnwlmsa2txujfhujb4sfahvg2a3bsntcbpyni3pywdp4m7khb4m \
  --churn-sha256 ac6d96c90353bd1253d121e48a03d4da06c326cc417e1a8dbf161bf8cfa8e1e3 \
  --churn-bytes 3658 --churn-blocks 1
```

Use current, independently verified public fixtures if those CIDs expire.
All four `--churn-*` pins are optional together: with them, most churn uses the
small file and every eighth pair uses the primary chunked file. Without them,
every pair fetches the large file. Defaults pause 30 seconds between pairs, sample
every five seconds and exclude the first 120 workload seconds from steady metrics.
No new pair starts in the final 180 seconds, leaving a quiet observation tail.

Every completed file and receipt must match the independent size, block-count and
SHA-256 pins. Verified files are then deleted; at most two output files are active.
There are no retries or silently ignored failures. Nonreader qualification requires
the daemon's specific content-write-timeout event, not a simulated stalled sink;
error-frame/close deadlines can extend connection cleanup beyond the first
five-second write deadline.

Evidence is private: binary/script/spec identities, bounded rotating role/event/
sample logs, and `summary.json` with failure counters, RSS/HWM/PSS, CPU and a
warmup-separated memory trend. CPU `100%` is one core. The harness stops and reaps
only its own children on completion, errors and handled signals. A missing summary
is not a pass. Do not rebuild its binaries during a run or publish raw evidence.
Offline harness checks: `python3 -B -m unittest discover -s scripts -p 'test_content_soak.py'`.

### Why the sharing evidence matters

One `smoldot_light::Client` owns the relay anchor, parachain anchors, and each
connection's RPC handle. Every parachain handle references the same relay anchor;
disconnecting an application never removes that anchor. Smoldot 4.0.0 deduplicates
chain services by its internal chain key while creating a distinct RPC frontend
on each `add_chain` call.

Its `Chain initialization complete` and `Parachain initialization complete` logs
are emitted only when the corresponding services are first created. The
acceptance script requires exactly one event for every configured chain, plus
two active sessions and actual chain reads. Session counts alone would not prove
sharing. The relay's checkpoint RPC frontend remains private.

See the pinned [upstream implementation](https://docs.rs/crate/smoldot-light/4.0.0/source/src/lib.rs)
(`chains_by_key`, `Entry::Occupied`, `Entry::Vacant`, and RPC frontend creation).

## IPC and safety boundaries

- Handshake paths are `/host`, `/rpc/<configured-chain-id>` and
  `/content/<configured-chain-id>`; unknown routes are rejected. There is no
  TCP/HTTP listener for browsers; `file://` pages cannot access it.
- Private socket directory, socket mode `0600`, and same-effective-UID checks on
  both ends. Browser-origin handshakes and unknown chain paths are rejected.
- Same-UID processes are not mutually sandboxed. Do not expose this prototype to
  untrusted users or treat it as a security boundary between your own applications.
- Maximum 16 connections, including discovery connections and pending handshakes;
  32 pending RPC requests and 8 subscriptions per chain session. Incoming frames
  are limited to 64 KiB and outgoing RPC messages to 8 MiB. Content has the tighter
  protocol limits above. Handshakes and blocked writes have a five-second deadline.
- No unbounded host-side forwarding queue. A blocked client loses its connection
  and must resubscribe, rather than silently losing ordered events. These transport
  limits are not a guarantee of a fixed total smoldot/runtime memory footprint.
- `rpc_methods` advertises only allowed read-only methods: supported `chainSpec_v1_*`
  and `chainHead_v1_*` operations. Transaction, administrative, legacy and raw
  Bitswap RPCs are unavailable through `/rpc`; content uses its separate bounded
  endpoint. Batch requests are not supported.
- Finalized state is the correctness boundary; best-block state is provisional.
  Complete historical data and full-node API parity are not provided.
- Never put wallet keys, seed phrases, private endpoints, or user-state dumps in
  this repository. The daemon does not hold signing keys.

## Public fixture provenance

`chains/paseo.json` is an unmodified public smoldot demo chain specification:

- [Source at revision 6aaa2a8e550f208b3d2182b411c99219680121e0](https://github.com/paritytech/smoldot/blob/6aaa2a8e550f208b3d2182b411c99219680121e0/demo-chain-specs/paseo.json)
- SHA-256: `72380e9c1515dbb35c30ecd4defe5689c4077a21c18eb8d19cf30dc72a328a00`
- Upstream [license](https://github.com/paritytech/smoldot/blob/6aaa2a8e550f208b3d2182b411c99219680121e0/LICENSE): GNU GPL v3.

`chains/paseo_asset_hub.json` is also unmodified:

- [Source at the same revision](https://github.com/paritytech/smoldot/blob/6aaa2a8e550f208b3d2182b411c99219680121e0/demo-chain-specs/paseo_asset_hub.json)
- SHA-256: `d50dc59e4d1f48f203542d0de2149928b26a640902ba8bd9351acdf64fed0b27`
- Route `asset-hub-paseo`, relay `paseo`, parachain ID 1000; same upstream license.

The Next v2 profile adds these unchanged compact specs from the same public
smoldot revision:

- [`paseo_asset_hub_next.json`](https://github.com/paritytech/smoldot/blob/6aaa2a8e550f208b3d2182b411c99219680121e0/demo-chain-specs/paseo_asset_hub_next.json)
  → `chains/paseo_next_asset_hub.json`.
- [`paseo_bulletin_next.json`](https://github.com/paritytech/smoldot/blob/6aaa2a8e550f208b3d2182b411c99219680121e0/demo-chain-specs/paseo_bulletin_next.json)
  → `chains/paseo_next_bulletin.json`.
- [`paseo_people_next.json`](https://github.com/paritytech/smoldot/blob/6aaa2a8e550f208b3d2182b411c99219680121e0/demo-chain-specs/paseo_people_next.json)
  → `chains/paseo_next_individuality.json`.

[`paseo-next-v2-provenance.json`](chains/paseo-next-v2-provenance.json) records
source SHA-256s, licenses, relay evidence and independently pinned
[SDK descriptor genesis hashes](https://github.com/paritytech/product-sdk/tree/964e6a3eee628c77bb294c0f7b62b210c841e9eb/product-sdk/packages/descriptors/chains).
No genesis root or checkpoint was derived from an untrusted public RPC response.

The pin makes this experiment reproducible; it is not an indefinitely fresh trust
checkpoint. Chain-spec/checkpoint updates need explicit provenance and review.

## Observed verification

Verified on x86-64 Linux with Rust 1.98.1 and systemd 261.

### Sustained diagnostics with the default allocator

A subsequent isolated run reused the verified diagnostic binaries with default
allocator settings, two concurrent large-file consumers and a one-second pause
between pairs. Every download used the same independently pinned file. A
task-local launcher added upstream Bitswap tracing alongside content debug logs;
no daemon code, retries, deadlines or production configuration changed. The
temporary scope retained the 1 GiB/no-swap safety limit.

| Measurement | Observed |
| --- | --- |
| Workload after finalized readiness, including quiet tail | 900 seconds |
| Independently verified downloads | 248, in 124 concurrent pairs |
| Unexpected failures / read errors / timeouts | 0 / 0 / 0 |
| Partial cancellation / deliberate writer timeout | 1 / 1, each followed by recovery |
| Sampled peak daemon RSS | 395.8 MiB |
| Post-warmup active RSS, first to last | 330.2 to 395.8 MiB |
| Last three active minutes' sampled RSS, first to last | 390.3 to 395.8 MiB |
| Post-warmup active CPU, one core = 100% | 12.16% average |
| Quiet-tail CPU, one core = 100% | 0.51% average |

After the final download, RSS changed by only 8 KiB across 170 sampled quiet
seconds. It still grew under late active load: neither this quiet plateau nor
the safety limit establishes a memory ceiling or absence of leaks. Trace logging
and live network conditions also prevent a controlled performance comparison
with earlier runs.

The daemon stopped cleanly, removed its socket and left no partial files.
Bounded, rotated logs retained correlated Have responses, selected-peer block
requests and payload-receipt metadata; they are a suffix, not a complete audit
of every transfer. The earlier upstream timeout did not recur and remains
unexplained. This is a passing 15-minute qualification run, not long-term or
mobile qualification. The separate six-hour chain-read baseline was still
active at the final check, with no completed result claimed. Raw evidence and
retained inputs remain outside the public repository.

### Content timeout diagnostics

All 35 focused Rust content tests passed, including preservation of upstream
timeout errors and the last stage when a pending response future is cancelled.
The 19 offline content-harness tests also passed; live writer-timeout recognition
was checked after adding contextual error messages.

An isolated live diagnostic scenario verified eight large-file downloads in four
concurrent pairs, partial cancellation, a deliberate nonreader writer timeout,
and recovery after each failure/cancellation case. Debug records correlated
requests, stages and byte counts; the daemon exited zero and removed its socket.

The unpublished-CID request returned `-32810` (no connected peers have the CID)
after 106 ms, with `phase=await_block`, one request attempt and no published or
remaining partial output. The throwaway check initially failed because it
incorrectly expected a timeout; the retained evidence was then evaluated against
the actual NotFound result without repeating network traffic. This verifies
diagnostic propagation, not reproduction or resolution of the earlier `-32811`
timeouts, and is not an endurance pass. Raw evidence remains private.

### Capability negotiation

Verified on 2026-11-14 with 57 focused release tests and an actual isolated daemon.
`/host` advertised the four configured content endpoints and enforced format/limit/
deadline values. The external consumer negotiated the Bulletin endpoint and saved
the independently pinned 10,493,449-byte file with its matching receipt and digest.
An unknown chain failed without output. Against the retained older daemon, ordinary
discovery still worked but the new content consumer rejected the missing capability
before opening a content session; it did not fall back to a guessed path.
Both temporary daemons stopped cleanly and removed their sockets.

### Chain-read soak failure and restart

The original requested six-hour classic Paseo relay/Asset Hub run **failed after
544 seconds of observed workload**. All nine completed churn probes exited zero,
but the harness treated output EOF before observable process exit as a failure.
Neither an auto-collected systemd unit nor its displayed inactive state establishes
a successful run; the failed JSON summary is authoritative.

The repaired harness grants only fully verified churn clients a five-second
exit-observation grace. Its 22 offline tests passed, including zero/nonzero exit,
missing completion and hung-process regressions. A separate 90-second live run
completed 15 churn probes and shut down cleanly. A new supervised six-hour run
was started with retained immutable baseline binaries and the repaired harness.
The replacement run completed the full six-hour workload on 2026-11-15. Its saved
summary reports success, all 359 short-lived consumers exited zero, and the daemon
saved its final checkpoint and exited zero. The continuous consumer's SIGTERM at
the requested end was intentional, not an unexpected failure.

| Measurement | Observed |
| --- | --- |
| Verified storage reads | 11,398 |
| Finalized notifications | 11,038 |
| Successful churn consumers | 359 / 359 |
| Steady average daemon CPU, one core = 100% | 0.55% |
| Sampled peak / final daemon RSS | 173.9 / 106.9 MiB |
| Checkpoint-save markers, including shutdown | 361 |

This qualifies the retained classic Paseo relay/Asset Hub chain-read baseline,
not six hours of the four-chain profile, Bulletin downloads or mobile operation.
RSS ended below its peak; the run does not establish leak freedom or a memory
ceiling. The completed JSON summary, not the subsequently absent transient unit,
is the result authority. Raw evidence remains private.

### Native Bulletin retrieval

On 2026-11-14, an isolated release daemon with the three-chain application profile
retrieved the sample CID above through native P2P Bitswap. The separate
`fetch-blob` process saved **10,493,449 bytes** from **10 blocks**: one DAG-PB
manifest and nine raw chunks. Retrieval took **6.965 seconds** after a finalized
Bulletin readiness probe; this is one smoke observation, not a throughput benchmark.

The output SHA-256 was
`91d59231ddad7d2e94c313aa5d9b95cc44b2a4775fdaaf24f6cd3f17f2cf1155`.
For independent fixture research, a public Bulletin full-node RPC supplied the
manifest and raw blocks; each CID digest and the ordered reconstruction were
checked separately with Node's SHA-256 implementation. The daemon/download
application did not use that RPC or an HTTP gateway.

The live smoke also verified:

- A 1024-byte caller limit rejects the large file without leaving partial output.
- Invalid CIDs fail and existing output is never overwritten.
- Ctrl-C after receiving 3658 bytes removes the partial file without publication.
- A subsequent 3658-byte raw-CID download succeeds; saved files have mode `0600`.
- The isolated daemon exits zero and removes its socket.

All **45 focused release tests** passed across content, native client and IPC.
They include hash/size failures, malformed protobuf, nested/repeated chunk ordering,
bounded traversal, cancellation during blocked writes, completion-marker/digest
validation and existing-file/symlink protection. The original long-running
finalized-read soak was not restarted or used for content traffic. This does not
establish long-term content-transfer stability or mobile performance.

### Content retrieval qualification

A subsequent isolated release run completed on 2026-11-14 after the review added
an encoded-DAG overhead budget. Its regression rejects repeated metadata-heavy
empty files without publishing content. All 15 focused DAG tests and 19 offline
content-harness tests passed; the release binaries/example built successfully.

| Measurement | Observed |
| --- | --- |
| Workload after finalized readiness | 900 seconds |
| Independently verified downloads | 50, in 25 concurrent pairs |
| Unexpected failures / read errors / timeouts | 0 / 0 / 0 |
| Partial-stream cancellation | 1, followed by successful recovery |
| Deliberate nonreader writer timeout | 1, followed by successful recovery |
| Steady daemon CPU, one core = 100% | 0.74% average |
| Steady RSS, first to last / peak | 259.9 to 297.5 MiB / 297.5 MiB |
| Steady PSS, first to last / peak | 256.6 to 294.1 MiB / 294.1 MiB |

The two-minute warmup is excluded from steady metrics. Traffic was intermittent:
small-file session churn, periodic large-file pairs, and a final three-minute
quiet tail. RSS rose during the earlier large-file cycles, then changed by less
than 0.5 MiB across the final six minutes. This does not identify the cause of
retained memory or prove the absence of leaks under sustained large-file traffic.
The harness completed cleanup without forced kills or shutdown failures; the
separate original chain-read soak was untouched. Raw evidence stays outside the
public repository. This is prototype qualification, not a production memory bound.

### Sustained large-file memory investigation

Two further runs used identical retained `c5d90ea` release binaries, the four-chain
profile, two concurrent consumers and the same verified 10,493,449-byte file for
every download. Pairs were separated by one second. Both requested 900 seconds,
including a final quiet period, inside separate 1 GiB/no-swap transient scopes.
Only the second run set the process-local glibc option `MALLOC_ARENA_MAX=2`.
Production configuration was unchanged.

| Measurement | Default allocator | Two arenas |
| --- | ---: | ---: |
| Observed workload (seconds) | 370.9, failed | 900.0, passed |
| Verified downloads | 88 | 198 |
| Sampled peak daemon RSS (MiB) | 349.9 | 160.4 |
| Kernel lifetime RSS high-water mark (MiB) | 349.9 | 163.6 |
| RSS at first sample after 88 verified downloads (MiB) | 349.9 | 155.0 |
| CPU in the post-warmup matched-count window (% of one core) | 10.4 | 11.8 |

The default run stopped because both final consumers received upstream Bitswap
request timeouts; it was not killed by the memory limit. The cause of those
timeouts remains undetermined. That run is a failed stability result, regardless
of its successful earlier downloads or clean daemon shutdown.

The two-arena run verified every started download, recovered after deliberate
cancellation and nonreader tests, and completed cleanup successfully. Its final
RSS remained 156.1 MiB over 170 sampled seconds after the last completed download.

[INFERENCE] The matched-count memory difference supports allocator retention as
a contributor, not proof of leak freedom. These public-network runs partly
overlapped and have unequal durations; their CPU figures are not a controlled
performance benchmark. Keep allocator tuning experimental pending longer,
repeatable concurrency and mobile measurements. Raw evidence remains private.

### Paseo Next v2 application profile

Verified the actual installer and socket-activated user units with the new default
profile, using a private checkpoint-directory override. Discovery exposed the
relay and all three correctly pinned parachains. The separate consumer read:

- Asset Hub's public Alice native account balance.
- Bulletin's stored retention policy: **201600 blocks**.
- Individuality's verified membership old-root grace-period constant: **600 seconds**.

The service saved a relay checkpoint and exited zero; the test installation and
override were removed, while private evidence/state were retained. Eight profile
example tests and five classic balance example tests passed. The multichain
harness's 18 focused checks passed after correcting a missing test-fixture setup.

Two fresh 180-second workload runs used identical release daemon/client binaries,
5-second sampling, 60-second warmup exclusion and 60-second client churn on the
same i7-10700K desktop. They ran concurrently; the earlier six-hour classic soak
was left untouched. Both completed, saved checkpoints and stopped cleanly:

| Measurement | Classic relay + Asset Hub | Relay + all three Next v2 app chains |
| --- | ---: | ---: |
| Sampled peak daemon RSS (MiB) | 106.7 | 187.4 |
| Steady average CPU (% of one core) | 0.55 | 0.74 |
| Verified continuous storage reads | 94 | 227 |
| Peak file descriptors | 33 | 40 |

All three app chains advanced independently: 96 Asset Hub, 32 Bulletin and 96
Individuality finalized notifications. These short runs are not long-term
stability, mobile/battery qualification or a memory ceiling. The profiles also
use different Asset Hub runtimes; the delta is not a per-chain memory budget.

### Earlier verification

The balance application and endurance harness were verified with release builds:

- Five metadata/address decoding tests and sixteen resource/progress harness
  tests passed.
- Two independent balance consumers read a present public test account and an
  absent account across successive finalized Asset Hub blocks using verified V14
  metadata. Absent balances were explicitly zero; malformed addresses failed.
- A 180-second active-workload smoke completed 97 continuous verified reads and
  96 advancing finalizations, plus two successful short-lived clients. The
  daemon saved its checkpoint and exited zero; the continuous probe was
  intentionally terminated at the test deadline.
- On an Intel i7-10700K desktop, that short run observed roughly 114 MiB peak
  daemon RSS and 0.70% steady average CPU (100% is one core). The final RSS
  sample was roughly 65 MiB. These are short, workload-specific observations,
  not a memory ceiling or evidence of long-term leak freedom.

Longer runs must be judged from their completed `summary.json`, not an initial
`SOAK_READY` marker. Evidence/checkpoints remain private and outside build outputs.

The Smolhost rename was verified with the renamed binaries and actual user units:

- Offline build and nine checkpoint tests passed. The lockfile changed only the
  project's package identity; all dependency entries stayed unchanged.
- Two live Asset Hub clients shared one relay and one parachain initialization,
  with verified reads continuing after the first client exited.
- `smolhost chains` activated `smolhost.service` through `smolhost.socket`.
  An installed probe read finalized Asset Hub block 15,745,942.
- The new socket/state paths had private permissions; a restarted daemon restored
  its checkpoint. Uninstall preserved state and removed the installation.

The test-created checkpoint directory and temporary harness were removed. No
service was left installed or running. Existing installations must follow the
upgrade instructions above; no old-name aliases are installed.

Earlier verification:

The live Asset Hub acceptance run passed against public peers:

- Exactly one relay and one parachain initialization, with two simultaneous
  application sessions on Asset Hub.
- Both clients read verified four-byte `System.Number` values and exited
  successfully. Client A finished at parachain block 15,743,206; after A exited,
  client B returned verified storage at block 15,743,209.
- Both sessions were released and the temporary daemon stopped.
- Eight client, fourteen IPC and eight engine tests passed, covering cancellation,
  ordered notifications, bounds, discovery errors, route isolation and configuration.

A separate live discovery check exercised the `chains` command, command-specific
argument validation, and raw RPC with Ctrl-C shutdown. A separately compiled Rust
consumer discovered both chains, selected Asset Hub by pinned genesis, connected
through the public adapter and read its actual name/properties. The temporary
consumer source, harness and daemon were removed; no persistent service was installed.

Earlier installed two-chain service checks also passed:

- Simultaneous relay and Asset Hub reads returned distinct genesis hashes, with
  one initialization per chain. An unconfigured route returned HTTP 404.
- Clean shutdown saved relay block 1,938,416. On restart smoldot reported that
  database starting point, and Asset Hub advanced from block 15,742,183 before
  restart to 15,742,198 afterward. Only the relay checkpoint was written.
- Uninstall preserved the checkpoint. Receipt-based removal also preserved an
  unrecorded file when exercising an older installation's file set.

The test installation and temporary checkpoints were removed; no service was left
running. The service test used a private temporary state-directory override,
retaining the packaged relay/parachain arguments and `0700` directory mode.

Earlier relay-only checks also exercised periodic/clean-shutdown checkpoint
restoration, automatic recovery after `SIGKILL`, preservation of the socket inode,
exclusive checkpoint writers, corrupt-state rejection, `--no-state`, and installer
ownership checks. Nine state tests passed during that checkpoint implementation.

This does not establish mobile compatibility, reboot/suspend behavior, or
production security.

## Development

Keep the implementation small: one Rust package, two binaries, and engine, IPC,
transport, and checkpoint modules. [AGENTS.md](AGENTS.md) contains the short working rules. Questions,
findings, and contributions are welcome in
[GitHub issues](https://github.com/replghost/smolhost/issues). Explain what
you changed and tested; do not post secrets or sensitive vulnerability details.

The next experiment is actual AArch64/mobile resource and suspend/resume
validation, not an automatic rollout of every system chain.

## License

Original project material is licensed under **GPL-3.0-or-later**; see [LICENSE](LICENSE).
Third-party material retains its upstream license and notices.

`smoldot-light` 4.0.0 declares `GPL-3.0-or-later WITH Classpath-exception-2.0` in its
[published manifest](https://docs.rs/crate/smoldot-light/4.0.0/source/Cargo.toml).
That exception applies to upstream material; this project does not grant it for
its own code. Review resolved dependency licenses before distributing binaries.
