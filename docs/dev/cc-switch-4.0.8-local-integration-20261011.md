# CC Switch 4.0.8 local compatibility integration

## Baselines and scope

- Official v4.0.8: `df40d53c36b604e044b4d5b9091aeb961aa56990`.
- Minimal bridge patch, retained as the integration starting point: `b8f1b68def5e53561548612f72c8377e84c3623e`.
- Previous local build: `codex/integration-4.0.4-20261008`, `dc80a9f961d0224fa19350117dc07f94643ac472`.
- Previous official base: `f9db9f7056cbe7f972cdc02644722002316866b9`.
- New candidate: `codex/integration-4.0.8-20261011`, separate from upstream PR #8120.

Useful existing custom behavior is retained unless the new official implementation demonstrably covers it. A disabled proxy is not a reason to discard proxy compatibility; maintainer rejection of a private-cache implementation is not equivalent official coverage.

This work only changed cloud source and synthetic fixtures. No user computer, installed app, live Codex config/database, account or real Desktop cache was accessed or replaced. No new upstream PR, merge, installer or release is included.

## Migration inventory

| Behavior | Official coverage and migration decision | Source and tests |
| --- | --- | --- |
| App/tui delegation envelope | Encrypted `agent_message` handling does not normalize plaintext `function_call_output` without a call ID. Restore the strict legacy guard before each actual provider attempt and before protocol conversion. | `proxy/codex_delegation.rs`, `forwarder.rs`; 10 tests for Chat/Anthropic conversion, idempotence, envelope preservation, official exclusion and malformed/explicit IDs. |
| Windows broken-npm repair | Official Windows repair still returns no command. CLI discovery and batch encoding are different fixes. Restore bounded `.cmd`/`.ps1` + real sibling npm repair, excluding native/Volta/pnpm/missing siblings. | `commands/misc.rs`; 5 Windows-only tests, including synthetic command continuation/final-status checks. Not executed on Linux. |
| Managed Chat/Anthropic missing ToolSearch | Retain the minimal parent patch and successful-attempt context. Suppress stale original identities when a saved `tools` override replaces the final tool surface. | `forwarder.rs`, `handlers.rs`, `transform_codex_chat.rs`; actual JSON/SSE loopback tests on both transports and all-transport override tests. |
| Native Responses ToolSearch | Official native passthrough/xAI flattening do not supply this complete transport. Restore native declaration conversion, history replay, typed IDs, loaded namespaces and JSON/SSE restoration using the actual successful request context. | `transform_codex_responses_namespace.rs`, handlers/forwarder; native JSON/SSE and search → loaded schema → namespaced invocation loopback tests, plus failover provenance. |
| Schema aliases and namespaces | #7454 already supports `additional_tools`, but not every search-output/schema/namespace case. Preserve it, normalize `inputSchema`/`input_schema` to `parameters`, merge repeated namespaces and keep original declared schemas authoritative. | Shared registry/native transformer; alias, overlapping-carrier, collision and schema-priority regressions. |
| Deferred loaded subset | Completed client `tool_search_output.tools` defines the loaded subset. Its defer metadata is removed only for that subset; unloaded siblings and ordinary deferred function/custom inventory stay deferred. | Registry/native tests cover parent namespace deferral, exact child loads, repeated results, incomplete/failed/server/nested outputs and no-tool turns. |
| Proxied catalog search capability | Direct conservative profiles and #6653 remain official-owned. Restore separate proxied native/Anthropic profiles only when the local adapter can supply search; saved tool/choice overrides suppress advertisement. | `codex_config.rs`, provider resolver, mode controller and Stack planner. Direct DeepSeek remains false; its vendor harness, hosted-tool restrictions and other metadata remain unchanged. |
| Reasoning fallback and Responses Lite | Preserve exact official/vendor rows and explicit user levels. Restore the old fallback only for its known unmatched model names and retain generic `use_responses_lite=false`. | Catalog tests for old known-model defaults, explicit overrides, official/vendor metadata and proxied profiles. No speculative new models. |
| Desktop private model allowlist | Public `model_catalog_json`, `/v1/models`, visibility and restart warnings do not prove equivalence to the separate Statsig allowlist. Retain old LevelDB reconciliation, ownership markers, rollback, lock-aware retries and generation cancellation in a separate commit. | `codex_config.rs`, `lib.rs`, `codex_direct.rs`, dependency lock; 25 cache fixtures (one Windows-only), plus a mode-switch fixture. |
| Auth/config/history continuity | No separate local auth/session-history writer delta exists in the old 20-file comparison. Preserve current official key-field writers, login/rollback, compressed history and direct/routing/Stack architecture. | Official code/tests remain. Protocol-level ToolSearch ID restoration is handled separately above. |
| Generic template loading | Current official code intentionally replaces cache-derived generic templates with a stable bundled classic contract and separate authoritative official-model discovery. Keep that replacement. | The absent old loader functions/tests were in the old official baseline, not newly introduced local functionality. |
| 3.x WebSocket bridge/diagnostics | Already intentionally omitted from the previous 4.0.4 integration. They are not newly lost here. | Preserve official local HTTP/SSE and WebSocket rejection; direct upstream WebSocket config remains official-owned. |

All 20 files changed by the old local integration are accounted for. Its old validation document remains with the old branch; this document describes the new candidate. The additional `responses_late_arguments.rs` change only preserves SSE framing during repair.

## Intentional safety differences

The old `tool_choice: "none"` → `"auto"` workaround is not retained. A loaded search result does not override an explicit no-tool request. Normal permitted discovery → invocation remains supported, including discovered definitions marked deferred.

Also excluded:

- Broad implicit injection from ordinary converters, empty requests, arbitrary nested output or bare history markers.
- Shim ownership guessed from a public function schema or application-owned `tool_search` name.
- Namespace-wide/unsupported Anthropic selectors silently degraded to `auto`. Unsupported restrictions produce an explicit error; exact named choices and supported allowed sets retain their restriction.
- Silent conversion of server-executed search to client execution. The compatibility conversion explicitly rejects unsupported server search.
- New native compatibility on official providers, GrokBuild, compact endpoints, compaction turns or saved tool/choice overrides. The legacy xAI namespace-only route remains separate.

A saved `tools` replacement owns its returned function identities on all transports. A choice-only xAI override still preserves the original flattened namespace identities while suppressing new injection.

Streams preserve stable `tsc_` IDs, split UTF-8/arguments, late argument ordering, SSE IDs/retry/comments/event fields and unknown framing fields. xAI integer normalization precedes item-type restoration. Compaction retains official #7976: definitions may remain for referenced history, while `tool_choice: "none"` prevents new calls.

Loaded-set semantics are documented in the [OpenAI ToolSearch guide](https://developers.openai.com/api/docs/guides/tools-tool-search).

## Private-cache risks

This is an unsupported, version-sensitive Desktop storage contract. Config IDs, schema, encodings, paths, renderer behavior or locks may change independently of the public API. A running Desktop may keep LevelDB locked; retries never bypass the lock. Current real Desktop picker behavior remains unverified.

The old scope is unchanged: existing discovered Codex storage only, no production cache creation, only dynamic config `107580212` model arrays, preserved unrelated values/encodings/pre-existing slugs, and removal only of previously marked injected slugs. External catalogs override stale inline models and preserve their pins. Model-only evaluations are pinned per origin, using the previous 30-day horizon, bounded discovery retries and weekly renewal. Superseded generations are cancelled.

Two pre-existing limitations are retained and disclosed:

1. Timestamp ownership is not separately recorded; official-mode cleanup can lower a future timestamp created by another writer on the same model-cache key.
2. A vendor later adding a slug already marked locally injected cannot be distinguished from the older locally owned slug.

Synthetic fixtures do not prove that the current Desktop still honors this private allowlist. A usable backup and Windows acceptance are required before installation.

## Validation

- Final-source Linux backend: **3,796 passed, 9 skipped**, across 17 binaries, Rust 1.95.0. Nextest run `54877083-a764-47bb-ae09-1abfcd043c4b`.
- Rust formatting and diff checks passed.
- Strict all-target Clippy with warnings denied passed (`cargo clippy --all-targets -- -D warnings`).
- Independent final source review found no outstanding actionable defects after restriction, override, route-isolation and streaming fixes.
- Frontend was unchanged, so its prior exact-pin validation was reused: Node 22.12.0 / pnpm 10.12.3, typecheck, formatting, **202 files / 2,433 tests**, renderer build. `src`, `tests`, package/lockfile and Node pin match `b8f1b68` exactly.
- Backend dependencies use the existing isolated Debian GTK/WebKit sysroot. Tests set isolated `CC_SWITCH_TEST_HOME`, `HOME`, `XDG_CONFIG_HOME`, `APPDATA` and `LOCALAPPDATA` and use synthetic databases/loopback upstreams.

The earlier 3,707-test minimal-patch backend result was not reused as proof of this broader integration. Intermediate regressions were fixed, then the complete final source was tested again.

## Remaining Windows acceptance

1. Windows build and Windows-only npm/launcher/cache-layout/filesystem tests.
2. Isolated Desktop/config/database validation: picker visibility, reasoning levels, direct/proxy transitions, external catalogs, locks and rollback.
3. Already-authorized read-only deferred tools through Chat, Anthropic and native Responses, including streaming/failover and loaded schema/namespace preservation.
4. Denied/no-tool/allowed-set/forced-tool requests remain restricted; saved tool overrides keep their identities.
5. Verify provider/config/database rollback before any installer. Official updates can replace local custom patches.

This is a cloud-validated upgrade candidate, not an installed upgrade or a claim of lossless current-Desktop compatibility.
