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
- Uses a pinned public Paseo relay-chain specification. No wallet or credentials
  are needed, and no transactions are submitted.
- Can run as an opt-in systemd user service, with a supervisor-owned Unix socket.
- Saves finalized bootstrap checkpoints every 60 seconds and on clean shutdown.

There is no parachain support, browser bridge, TCP listener, signing, or Zenoh
integration. Desktop and mobile are the intended targets; an x86-64 run does not
establish AArch64 or phone support.

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
./target/debug/polkadot-hostd --chain-spec chains/paseo.json
```

It creates `$XDG_RUNTIME_DIR/polkadot-host/rpc.sock` and connects to public Paseo
peers. Synchronization may take time; readiness of the socket does not mean the
chain is synchronized. No remote RPC fallback is used.

The bundled bootnodes use secure WebSockets. A small native platform adapter adds
WSS using Rustls and Mozilla trust roots from `webpki-roots`, with hostname and
certificate verification enabled. Smoldot's default native platform handles the
executor, clocks, randomness, and non-TLS connections. TLS does not replace
smoldot's peer authentication, finality verification, or storage-proof checking.

In another terminal:

```sh
./target/debug/polkadot-host --chain paseo probe --finalizations 3
```

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
~/.local/bin/polkadot-host --chain paseo probe --finalizations 3
```

Build both binaries first. The installer defaults to `target/debug`; use
`--bin-dir /absolute/build/output` for another build. It copies the binaries and
Paseo spec into `~/.local/lib/polkadot-host`, links the CLI in `~/.local/bin`, and
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

Each exact chain-spec fingerprint has a JSON checkpoint and a separate lock file.
Different spec bytes select a different checkpoint; old files are not deleted.
Only one daemon can own a given specification's state at a time, even when its
IPC socket differs. Directories must be user-owned `0700`; state files must be
user-owned `0600`. Symlink and nonregular-file endpoints are rejected.

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

Offline checks cover read-only method filtering, socket ownership/lifecycle,
checkpoint integrity, atomic replacement, permissions, and exclusive writers:

```sh
cargo test --locked --lib
```

For a live test against public Paseo peers, build both binaries first, then run:

```sh
python3 scripts/smoke.py --chain-spec chains/paseo.json --chain paseo
```

The script uses its own temporary directory and daemon. It requires:

1. Two separate CLI processes connected at the same time, both reading real state.
2. Client A completing and exiting while client B continues to read later finalized
   state and then completes successfully.
3. Both sessions being released without stopping the daemon.
4. Exactly one smoldot chain initialization across those connections.

The script uses `--no-state`, so it never touches your persistent checkpoints.
It prints a JSON result and stops its child processes on success, failure, or
interruption. It does not install anything, change a live service, or submit a
transaction. Use `--bin-dir` for a custom build output and `--timeout-secs` for a
longer overall deadline. Network availability is required; this is not an offline
unit test.

### Why the sharing evidence matters

One `smoldot_light::Client` owns an RPC-free anchor handle and the per-connection
handles. Smoldot 4.0.0 deduplicates chain services by its internal chain key, while
creating a distinct RPC frontend on each `add_chain` call. Its
`Chain initialization complete` log is emitted only when those services are first
created, not when another handle attaches. The acceptance script combines that
upstream event with two active sessions and actual chain reads; session counts
alone would not establish sharing.

See the pinned [upstream implementation](https://docs.rs/crate/smoldot-light/4.0.0/source/src/lib.rs)
(`chains_by_key`, `Entry::Occupied`, `Entry::Vacant`, and RPC frontend creation).

## IPC and safety boundaries

- Endpoint path in the WebSocket handshake: `/rpc/paseo` for the bundled chain.
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

The pin makes this experiment reproducible; it is not an indefinitely fresh trust
checkpoint. Chain-spec/checkpoint updates need explicit provenance and review.

## Observed verification

The initial live acceptance run passed on x86-64 Linux with Rust 1.98.1:

- One smoldot chain initialization and two simultaneous RPC sessions.
- Both clients read verified `System.Number` values and exited successfully.
- Client A finished at finalized block 1,932,326; after A exited, client B
  continued through block 1,932,329
  (`0x96e03c879d31019318d721710d8755939ad26f52eb9d8e4d55c34398659a69f7`).
- Both sessions were released; the script stopped its temporary daemon.

A subsequent systemd 261 user-manager run verified:

- Socket activation with two concurrent clients and one chain initialization.
- Graceful daemon restart and automatic recovery after `SIGKILL`, preserving the
  socket inode and accepting fresh clients afterward.
- Enable/disable without losing the installation; uninstall while enabled and
  running; protection of conflicting, modified, and unrelated files.
- Verified state through block 1,932,779 after installing the final unit layout.

All six offline IPC tests passed, including inherited-listener lifetime and
permission checks. The test installation was removed afterward. Login/reboot,
suspend/resume, and mobile behavior were not exercised.

Persistence verification subsequently passed against public Paseo peers:

- A periodic checkpoint captured finalized block 1,933,109. After `SIGKILL`,
  smoldot reported **Database starting at block 1,933,109**, then the probe read
  verified state at 1,933,110.
- Clean shutdown saved block 1,933,111; the restarted service resumed there and
  returned verified state at 1,933,112.
- Two application sessions still shared one chain initialization. A second
  checkpoint writer was refused, corrupt state remained untouched, and
  `--no-state` returned verified state without creating persistent files.
- All 17 offline tests passed across targeted runs. Temporary service units and
  test checkpoints were removed.

This does not establish mobile compatibility, reboot/suspend behavior, or
production security.

## Development

Keep the implementation small: one Rust package, two binaries, and engine, IPC,
transport, and checkpoint modules. [AGENTS.md](AGENTS.md) contains the short working rules. Questions,
findings, and contributions are welcome in
[GitHub issues](https://github.com/replghost/polkadot-host/issues). Explain what
you changed and tested; do not post secrets or sensitive vulnerability details.

The next experiments are a parachain and actual AArch64/mobile validation.
Neither is implied by a successful relay-chain run.

## License

Original project material is licensed under **GPL-3.0-or-later**; see [LICENSE](LICENSE).
Third-party material retains its upstream license and notices.

`smoldot-light` 4.0.0 declares `GPL-3.0-or-later WITH Classpath-exception-2.0` in its
[published manifest](https://docs.rs/crate/smoldot-light/4.0.0/source/Cargo.toml).
That exception applies to upstream material; this project does not grant it for
its own code. Review resolved dependency licenses before distributing binaries.
