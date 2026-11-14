# Working on polkadot-host

Read [README.md](README.md) for scope and current status. This is a small,
experimental side project; keep the code and process simple.

- Use only this repository and public sources. Do not consult private repositories
  or reuse private implementation details. Respect upstream licenses and attribution.
- Never publish secrets, wallet keys, private endpoints, or personal logs.
- Keep changes focused. Reuse smoldot and standard Polkadot JSON-RPC; avoid
  speculative abstractions, extra services, and unrelated cleanup.
- Preserve the README's security boundaries: no key custody, no silent remote-RPC
  fallback, and no weakening session isolation or resource limits to pass a demo.
- Verify the behavior you change. State what you ran and what remains untested;
  compiling for a phone does not mean it has been tested on one.
- Keep docs accurate, but do not create plans, reports, or process documents unless
  the task needs them. No mandatory AI disclosure or formal review ceremony.
- Ask before committing or publishing, deploying persistent services, submitting
  transactions, changing host/device settings, or deleting user data. Explicit
  authorization for the current task is sufficient; do not ask again for each step.
- Preserve unrelated work and follow local build-storage rules. Clean up temporary
  processes you started, not somebody else's.
