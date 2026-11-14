# polkadot-host

A shared Polkadot light-client service for Linux applications.

**Experimental, unofficial prototype.** Not security-audited or suitable
for production funds. Not affiliated with Parity Technologies or the Web3 Foundation.

## What this spike does

- Runs native [smoldot-light](https://github.com/paritytech/smoldot) in one daemon.
- Exposes read-only Polkadot JSON-RPC over WebSocket framing on a Unix socket.
- Gives each connection its own RPC session while sharing chain synchronization.
- Includes a CLI that follows finalized blocks and reads `System.Number` from
  verified chain storage, plus an opt-in two-process acceptance script.
- Loads pinned public Paseo relay and Asset Hub specifications in one client.
  No wallet or credentials are needed, and no transactions are submitted.
- Can run as an opt-in systemd user service, with a supervisor-owned Unix socket.
- Saves relay bootstrap checkpoints every 60 seconds and on clean shutdown.

There is no browser bridge, TCP listener, signing, or Zenoh integration. Desktop
and mobile are the intended targets; an x86-64 run does not establish AArch64 or
phone support.

## Chain defaults and application needs

The installed service currently enables **classic Paseo + Asset Hub Paseo**
(parachain 1000). It does not enable production chains. The relay is a shared
security dependency, not a separate light client per application or parachain.

For an application-oriented profile, prefer a small capability catalog rather
than automatically synchronizing every system chain:

| Role | What applications need it for | Policy |
| --- | --- | --- |
| Relay chain | Parachain verification and finality | Shared dependency |
| Asset Hub | Assets, balances, runtime/contract reads, naming where deployed | Current tested default |
| Bulletin | Content-addressed files, app bundles, media | Candidate for an app profile; blob retrieval also needs Bitswap/IPFS |
| Individuality / People deployments | Identity, personhood, statement allowances | Select the exact deployment; do not treat these names as interchangeable |
| Bridge Hub, Collectives, Coretime | Bridges, collectives/fellowship, coretime operations | Explicitly configure only when an application needs them |

The public [Product SDK chain client](https://docs.polkadot.com/apps/product-sdk/chain-client/)
uses Asset Hub, Bulletin, and Individuality as its three application-facing roles.
However, its [network presets](https://docs.polkadot.com/apps/concepts/networks/)
distinguish Paseo Next v2 from Products Devnet; neither is an alias for this
classic Asset Hub fixture. Match genesis hashes and runtime/deployment metadata,
not merely a friendly network name. This daemon is not a Product SDK Host provider.

[Individuality](https://github.com/paritytech/individuality-community) describes
itself as experimental People-chain infrastructure, not part of the official
People chain. [Bulletin](https://github.com/paritytech/polkadot-bulletin-chain) is a
storage parachain with authorization and retention rules; ordinary storage RPC
does not by itself deliver its content-retrieval service.

Beyond chain reads, applications may need:

- Chain/capability discovery, readiness, genesis checking, runtime metadata and
  a native IPC adapter for their client library.
- Wallet-mediated signing and explicit transaction approval/submission. Keys
  should stay outside this read-only daemon.
- CID-verified blob retrieval and bounded caching for Bulletin; signed-statement
  gossip, allowances and expiry handling for applications using Statement Store.
- Network-specific contract/name-service configuration, and optional indexers for
  history/search that a light client does not provide.
- Reconnection/resubscription and an idle/suspend policy appropriate for phones.

These are capability boundaries and recommendations, not implemented APIs.
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
./target/debug/polkadot-hostd --chain-spec chains/paseo.json \
  --parachain-spec chains/paseo_asset_hub.json
```

It creates `$XDG_RUNTIME_DIR/polkadot-host/rpc.sock` and connects to public Paseo
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

In another terminal:

```sh
./target/debug/polkadot-host --chain asset-hub-paseo probe --finalizations 3
```

Use `--chain paseo` to query the shared relay instead. The corresponding handshake
paths are `/rpc/asset-hub-paseo` and `/rpc/paseo`. A connection selects one chain
once; its requests and subscriptions remain isolated from other connections.

The probe prints JSON lines: `initialized`, `storage`, `finalized`, and `completed`.
It reads a four-byte SCALE `System.Number` at the initial finalized block and after
each subsequent finalized notification, verifies increasing numbers, and releases
obsolete block pins. Its default total deadline is 240 seconds; use
`--timeout-secs` to change it. Errors, disconnection, or a stopped subscription
fail the probe rather than being hidden by retries.

For raw RPC, the CLI streams responses until disconnect, Ctrl-C, or its deadline:

```sh
./target/debug/polkadot-host --chain paseo rpc \
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

## Run as a user service

Requires Linux with a running systemd user manager. No root privileges or sudo:

```sh
python3 scripts/user-service.py install
systemctl --user enable --now polkadot-host.service
~/.local/bin/polkadot-host --chain asset-hub-paseo probe --finalizations 3
```

Build both binaries first. The installer defaults to `target/debug`; use
`--bin-dir /absolute/build/output` for another build. It copies the binaries and
Paseo and Asset Hub specs into `~/.local/lib/polkadot-host`, links the CLI in `~/.local/bin`, and
copies the units into `$XDG_CONFIG_HOME/systemd/user` (normally
`~/.config/systemd/user`). Nothing runs from disposable build outputs.

Installation alone **does not start or enable anything**. The explicit
`enable --now` command starts the daemon and enables both units for future user
sessions. For this session only, use `systemctl --user start polkadot-host.service`.
For on-demand activation instead, enable only `polkadot-host.socket`; the first
connection starts the daemon, which then stays running.

The socket remains `$XDG_RUNTIME_DIR/polkadot-host/rpc.sock`, mode `0600`, in a
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
systemctl --user status polkadot-host.service polkadot-host.socket
journalctl --user -u polkadot-host.service -f
systemctl --user restart polkadot-host.service
```

To stop it, stop **both** units; leaving the socket running allows connections to
reactivate the daemon:

```sh
systemctl --user stop polkadot-host.socket polkadot-host.service
```

To also disable future startup, use `systemctl --user disable --now
polkadot-host.socket polkadot-host.service`. This keeps the installation.
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

## Bootstrap checkpoints

Persistence is on by default, including manual runs. State lives in
`$XDG_STATE_HOME/polkadot-host` (normally `~/.local/state/polkadot-host`), outside
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

Offline checks cover chain/parent configuration, route isolation, read-only method
filtering, socket lifecycle, checkpoint integrity and exclusive writers:

```sh
cargo test --locked --lib
```

For a live test against public Paseo peers, build both binaries first, then run:

```sh
python3 scripts/smoke.py --chain-spec chains/paseo.json \
  --parachain-spec chains/paseo_asset_hub.json --chain asset-hub-paseo
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

- Handshake paths are `/rpc/<configured-chain-id>`; unknown routes are rejected.
  There is no TCP/HTTP listener for browsers; `file://` pages cannot access it.
- Private socket directory, socket mode `0600`, and same-effective-UID checks on
  both ends. Browser-origin handshakes and unknown chain paths are rejected.
- Same-UID processes are not mutually sandboxed. Do not expose this prototype to
  untrusted users or treat it as a security boundary between your own applications.
- Maximum 16 connections, including pending handshakes; 32 pending RPC requests
  and 8 subscriptions per session; incoming messages limited to 64 KiB and outgoing
  messages to 8 MiB. Handshakes and blocked writes have a five-second deadline.
- No unbounded host-side forwarding queue. A blocked client loses its connection
  and must resubscribe, rather than silently losing ordered events. These transport
  limits are not a guarantee of a fixed total smoldot/runtime memory footprint.
- `rpc_methods` advertises only allowed read-only methods: supported `chainSpec_v1_*`
  and `chainHead_v1_*` operations. Transaction, administrative, and legacy RPCs are
  unavailable. Batch requests are not supported.
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

The pin makes this experiment reproducible; it is not an indefinitely fresh trust
checkpoint. Chain-spec/checkpoint updates need explicit provenance and review.

## Observed verification

Verified on x86-64 Linux with Rust 1.98.1 and systemd 261.

The live Asset Hub acceptance run passed against public peers:

- Exactly one relay and one parachain initialization, with two simultaneous
  application sessions on Asset Hub.
- Both clients read verified four-byte `System.Number` values and exited
  successfully. Client A finished at parachain block 15,741,964; client B
  continued through blocks 15,741,965–15,741,967 after A exited.
- Both sessions were released and the temporary daemon stopped.
- Eight engine and nine IPC tests passed, including invalid parent/ID rejection
  and configured-route selection.

The installed two-chain service also passed live checks:

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
[GitHub issues](https://github.com/replghost/polkadot-host/issues). Explain what
you changed and tested; do not post secrets or sensitive vulnerability details.

The next experiments are application-facing discovery/client adapters and actual
AArch64/mobile validation, not an automatic rollout of every system chain.

## License

Original project material is licensed under **GPL-3.0-or-later**; see [LICENSE](LICENSE).
Third-party material retains its upstream license and notices.

`smoldot-light` 4.0.0 declares `GPL-3.0-or-later WITH Classpath-exception-2.0` in its
[published manifest](https://docs.rs/crate/smoldot-light/4.0.0/source/Cargo.toml).
That exception applies to upstream material; this project does not grant it for
its own code. Review resolved dependency licenses before distributing binaries.
