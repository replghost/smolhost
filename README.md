# polkadot-host

A shared Polkadot light-client service for Linux applications.

**Experimental, unofficial, manual-run spike.** Not security-audited or suitable
for production funds. Not affiliated with Parity Technologies or the Web3 Foundation.

## What this spike does

- Runs native [smoldot-light](https://github.com/paritytech/smoldot) in one daemon.
- Exposes read-only Polkadot JSON-RPC over WebSocket framing on a Unix socket.
- Gives each connection its own RPC session while sharing chain synchronization.
- Includes a CLI that follows finalized blocks and reads `System.Number` from
  verified chain storage, plus an opt-in two-process acceptance script.
- Uses a pinned public Paseo relay-chain specification. No wallet or credentials
  are needed, and no transactions are submitted.

There is no systemd integration, persisted bootstrap state, parachain support,
browser bridge, TCP listener, signing, or Zenoh integration in this spike. It
starts from the chain specification on every launch. Desktop and mobile are the
intended targets; an x86-64 run does not establish AArch64 or phone support.

## Build

A Linux environment and a Rust toolchain supporting edition 2024 are required.
Python 3 is needed only for the acceptance script. The lockfile fixes dependency
versions, including `smoldot-light` 4.0.0 and its native runtime dependencies.

```sh
cargo build --locked --bins
```

Build artifacts go in `target/` unless your local build tooling overrides that
location. Source and chain state must not be stored in disposable build outputs.

## Run

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
users. An existing socket is never replaced. Stop the daemon with Ctrl-C or SIGTERM;
it cancels its sessions and removes its own socket. After an unclean crash, verify
that no daemon owns the endpoint before manually removing a stale socket.

## Verify

Offline checks cover read-only method filtering and socket ownership/lifecycle
boundaries:

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

This proves the manual relay-chain scenario, not service installation, restart
persistence, mobile compatibility, or production security.

## Development

Keep the implementation small: one Rust package, two binaries, and engine/IPC/
transport modules. [AGENTS.md](AGENTS.md) contains the short working rules. Questions,
findings, and contributions are welcome in
[GitHub issues](https://github.com/replghost/polkadot-host/issues). Explain what
you changed and tested; do not post secrets or sensitive vulnerability details.

After the manual two-client spike, the next experiments are systemd user-service
integration, persisted bootstrap state, a parachain, and actual AArch64/mobile
validation. None are implied by a successful relay-chain run.

## License

Original project material is licensed under **GPL-3.0-or-later**; see [LICENSE](LICENSE).
Third-party material retains its upstream license and notices.

`smoldot-light` 4.0.0 declares `GPL-3.0-or-later WITH Classpath-exception-2.0` in its
[published manifest](https://docs.rs/crate/smoldot-light/4.0.0/source/Cargo.toml).
That exception applies to upstream material; this project does not grant it for
its own code. Review resolved dependency licenses before distributing binaries.
