# CC Switch 4.0.4 local compatibility integration

## Baseline and scope

- Official baseline: `origin/main@f9db9f7056cbe7f972cdc02644722002316866b9`, package version 4.0.4.
- This baseline is six commits after the official `v4.0.4` release tag, including Codex GitHub Copilot managed-account support.
- Keep the official key-field configuration writers, direct/routing/aggregation architecture, compaction support and database v20 migration.
- Update PR #5265 (Desktop catalog/cache compatibility) and PR #5799 (missing ToolSearch/deferred tools compatibility) independently, preserving their existing fork branches.
- Retain the narrowly scoped delegation-input normalization from the installed 3.20.4 build. Run it on each actual Codex provider attempt before protocol conversion; official providers and ordinary tool results remain unchanged.
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
- PR #5265, combined backend, package and runtime validation: pending at the time of this initial record.

## Packaging and rollback

Build a local unsigned NSIS installer with updater artifact generation disabled. Keep the installed 3.20.4 program and real configuration/database untouched. Archive installer, app executable, hashes and this validation record in a new directory under `E:\CC-Switch-Archive`.

Database migration is v19 to v20. Returning to 3.x requires restoring a compatible pre-upgrade database backup; swapping the executable alone is insufficient. Official future updates can replace local compatibility patches.
