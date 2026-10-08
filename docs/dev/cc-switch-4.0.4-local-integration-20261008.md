# CC Switch 4.0.4 local compatibility integration

## Baseline and scope

- Official baseline: `origin/main@f9db9f7056cbe7f972cdc02644722002316866b9`, package version 4.0.4.
- This baseline is six commits after the official `v4.0.4` release tag, including Codex GitHub Copilot managed-account support.
- Keep the official key-field configuration writers, direct/routing/aggregation architecture, compaction support and database v20 migration.
- Update PR #5265 (Desktop catalog/cache compatibility) and PR #5799 (missing ToolSearch/deferred tools compatibility) independently, preserving their existing fork branches.
- Retain the narrowly scoped delegation-input normalization from the installed 3.20.4 build. Run it on each actual Codex provider attempt before protocol conversion; official providers and ordinary tool results remain unchanged.
- Retain Windows broken-npm Codex platform-package repair, which is still absent from official Windows code. Narrow it to `.cmd`/`.ps1` launchers with a real sibling npm; native EXEs, Volta, pnpm and missing siblings keep their existing update paths. This only generates the repair command for an explicit tool upgrade; no installed CLI is upgraded during this integration.
- Do not carry forward the old WebSocket bridge, obsolete full-config takeover writers, temporary route-observation switches or historical diagnosis files.

## WebSocket migration decision

The current machine has all CC Switch proxy/takeover flags disabled, while the live Codex provider advertises `supports_websockets = true`. Direct client-to-upstream WebSocket use does not depend on CC Switch's local bridge.

HTTP Responses supports streamed text and tool calls. WebSocket Responses keeps a connection open across turns and can lower continuation overhead for long tool-call chains; newer official protocol features also include `stream_id` multiplexing. Neither transport alone guarantees that a particular gateway supports every tool or model feature.

The old local bridge is a minimal native Responses implementation. Its restoration state is connection-wide; it has no explicit per-stream state, aggregation provider switching or mid-turn steering implementation. Porting it unchanged would add a separate route that is inconsistent with the new official mode and aggregation machinery. No performance advantage for this machine was benchmarked.

Decision: retain official HTTP local routing and its explicit WebSocket rejection. Direct WebSocket capability is retained by the official direct configuration path. A complete bridge can be reconsidered if local-routing WebSocket latency becomes a measured requirement.

Sources:

- https://developers.openai.com/api/docs/guides/websocket-mode
- Official `src-tauri/src/proxy/handlers.rs`: `handle_responses_websocket`.
- Installed-build source `1f154a1f:src-tauri/src/proxy/responses_websocket.rs`.

## Validation record

- PR #5799: 3445 Rust library tests passed, 10 ignored, 2 Windows symlink tests excluded after both reproduced privilege error 1314; format and all-target Clippy passed.
- The Copilot regression additionally checks that proxy catalog selection retains the managed Copilot profile across transport and format overrides.
- Common official frontend: typecheck, Prettier, 196 test files / 2304 tests, and production renderer build passed. Frontend tree matches the official baseline.
- PR #5265: 3425 Rust library tests passed, 10 ignored, the same 2 environment tests excluded; format and all-target Clippy passed.
- Updated PR heads: #5265 `86d34a28`; #5799 `fa1b8cbe`. Both include the official baseline, were pushed without rewriting remote history, and GitHub reports them mergeable.
- Combined resolution retains both known-model reasoning metadata fallback and proxied ToolSearch capabilities. Both mode-controller regressions remain enabled.
- Final combined backend: all 17 Rust target suites completed with 3662 passed, 0 failed, 10 ignored, and 3 environment tests excluded. Library tests account for 3481 of the passes; the remaining 181 are integration/golden tests.
- Excluded tests: `resolve_catalog_rejects_symlink_escaping_config_dir`, `symlink_cycle_does_not_cause_stack_overflow`, and `sync_to_app_removes_disabled_and_orphaned_ssot_symlinks`. Each reproduced Windows privilege error 1314 while constructing its symlink fixture. They are not counted as passes.
- Changed backend files passed strict UTF-8/no-BOM checks; frontend source and test Git trees match the official baseline.
- Final all-target Clippy with warnings denied and Rust format check passed.
- A separate debug application identity (`com.ccswitch.localcompat.test.20261008`) started successfully with an empty isolated home, Codex home, AppData and LocalAppData. It remained alive for the bounded 20-second check and was then stopped. Its database reached schema 20 and SQLite `quick_check` returned `ok`.
- The real Codex configuration SHA-256 was identical before and after the startup check. The installed 3.20.4 executable was not replaced and its real database was not migrated.
- This is a startup check, not visual UI acceptance or real-upstream/client routing acceptance. Production installer execution and updater behavior are unverified.
- Production NSIS build result, executable metadata and archive hashes are recorded separately in the archive README after packaging.

## Packaging and rollback

Build a local unsigned NSIS installer with updater artifact generation disabled. Keep the installed 3.20.4 program and real configuration/database untouched. Archive installer, app executable, hashes and this validation record in a new directory under `E:\CC-Switch-Archive`.

Database migration is v19 to v20. Returning to 3.x requires restoring a compatible pre-upgrade database backup; swapping the executable alone is insufficient. Official future updates can replace local compatibility patches.
