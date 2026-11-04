# Local v4.0.25 integration

## Fixed source and delivery scope

- Official release baseline: `v4.0.25`, commit `a3eafa408ae679db2e3401fca767ed2b89d9a2b2`.
- Previous personalized release: `local-v4.0.17-20261021`, commit `5e54da9a136b8060bb793a8558eb03502ee43fe0`.
- The integration starts from the official release and merges the complete retained personalized lineage. Later upstream main commits are excluded.
- Windows x64 executable and NSIS installer are built in the fork's dedicated Actions workflow. They are unsigned personal builds, not official publisher-signed binaries. The user installs them manually; this task does not touch the installed application or production configuration.
- The release includes exact source, retained patch, checksums, build provenance and validation logs. GitHub Actions checks and the downloadable manifest are authoritative for the final build result. A source integration alone is not evidence that compilation, tests or desktop acceptance passed.

## Changes from 4.0.17 to 4.0.25

1. New application support: Antigravity CLI, Kimi Code, Oh My Pi and DeepSeek Harness, including their supported provider/configuration, MCP, Skills, session and usage integrations. Antigravity provider selection applies to terminals opened from its CC Switch card.
2. Account and daily workflows: multiple Claude subscription accounts, browser login for managed ChatGPT accounts, optional automatic browser opening, account quota refresh, application ordering, provider model ordering and terminal launch options.
3. Codex: compaction history separated by tool protocol, selected-provider base URL parsing, new subagent concurrency setting, optional official quota protection, model mapping usage deduplication and forked-session usage fixes.
4. MCP connectivity checks, exchange-rate auto-fetch opt-in, clearer Claude model/provider labels, Kimi session deletion and more accurate usage/cost calculations.
5. Security and stability: local routing rejects browser-origin requests and self-forwarding, Windows form autofill flicker and lightweight-window recovery fixes, tray startup responsiveness and multiple protocol/media conversion repairs.

See the official Chinese notes for [4.0.18](../release-notes/v4.0.18-zh.md), [4.0.19](../release-notes/v4.0.19-zh.md), [4.0.20](../release-notes/v4.0.20-zh.md), [4.0.21](../release-notes/v4.0.21-zh.md), [4.0.22](../release-notes/v4.0.22-zh.md), [4.0.23](../release-notes/v4.0.23-zh.md), [4.0.24](../release-notes/v4.0.24-zh.md) and [4.0.25](../release-notes/v4.0.25-zh.md).

## Personalized patch audit

No complete retained personalized feature was found to be equivalently incorporated upstream. Partial upstream improvements are retained alongside the personalized increments.

| Feature | Evidence in official 4.0.25 | Integration |
| --- | --- | --- |
| Built-in `openai` identity and Dot compatibility | Official direct-route plan still uses the custom bucket and OfficialMirror | Retain opted-in API identity repair, native-auth preservation and built-in history migration |
| Portable web-search history | No `codex_portable_history` equivalent; history migration unchanged from .17 | Retain request, JSON and SSE projection, scoped backup and migration |
| Missing ToolSearch declarations | No equivalent request-scoped declaration shim | Retain existing narrow gates and tests; closed unmerged PR #8120 is not evidence of an upstream replacement |
| Native Responses search and namespaces | Namespace transform unchanged from .17; existing foundations only partially overlap | Retain typed IDs, schema aliases, duplicate namespace merge, loaded subsets, successful-attempt contexts and JSON/SSE restoration |
| Proxy model catalogs | Official direct profiles do not cover local proxy adapters | Retain ProxyNativeResponses/ProxyAnthropic; also retain official tool-mode-separated compaction hashes |
| Plaintext delegation | Official orphan output handling does not replace strict desktop envelope normalization | Retain bounded adapter and official orphan/media handling |
| Windows npm platform-package repair | Official Windows `codex_repair_command` still returns None | Retain bounded sibling npm repair; retain official install-exit-status eligibility |
| Embedded production UI validation | Local read-only verification is not supplied upstream | Retain main-window and tray-panel asset checks |
| Known-model reasoning fallback, Responses Lite and directory search visibility | No equivalent complete removal justification in changed official areas | Retain existing behavior without broadening gates |

Both textual conflicts were additive: retain upstream startup/file-limit helpers and the local application-context helper; retain all three proxy modules (official quota protection and local delegation/portable history). Rust module ordering is formatted normally.

Important coexistence checks: official quota protection stays scoped to actual official providers, not merely desktop `openai` identity; direct terminal catalogs stay distinct from proxy profiles; new Chat completion envelope unwrapping and orphan media marker remain official; xAI encrypted-content filtering remains alongside local restoration.

## Explicit exclusions and authority boundaries

- No LevelDB/Statsig/private Desktop cache reading, writing, pinning or rewriting is restored.
- `tool_choice: none` is never changed to `auto`. Allowed/forced tools are not broadened.
- Hosted/server ToolSearch rejection and application-owned-name collision guards remain intact.
- No upstream repository changes, real-client configuration changes, automatic installation or GUI acceptance claims.
- The original updater endpoints/public key stay unchanged. Updating through the official updater could replace the personalized build; use the fork's manually downloaded builds to retain personalized patches.

## Database upgrade and rollback

The official database schema changes from **20 to 24**. The application attempts a pre-migration database backup, but its code logs a warning and continues if that backup fails. Therefore do not rely on that attempt as the only backup.

Before first launch, fully exit CC Switch and make a separate copy of its configuration directory (including `cc-switch.db`, any adjacent SQLite WAL/SHM files and configuration files). Keep the 4.0.17 installer and that backup together. Also retain the Codex configuration/history backup if you rely on history migration. Do not upload these private configuration/history files to GitHub.

A 4.0.17 binary rejects a database newer than schema 20. Rollback requires restoring the pre-upgrade data backup while the application is closed, not merely replacing the executable. Changes made after upgrading are not automatically backported to the old database.

## Validation and release retention

The fork-only Windows workflow must pass frontend type checking, formatting, unit tests, backend formatting, strict Clippy and backend tests before publishing. Production packaging and embedded UI checks run after validation. Logs are uploaded even on failure; a failed run must not be called a successful release. Live Windows Desktop/Codex acceptance remains the user's separate installation check.

Retain the new verified 4.0.25 release and previous 4.0.17 release. Do not remove 4.0.15 before the new assets and checksums have been verified. Source tags/history are preserved independently of release asset retention; any cleanup must respect recoverability and authorization.

Cloud pre-publication checks: frontend TypeScript, Prettier and production renderer build passed; Vitest passed **3,535 / 3,535 tests in 274 files**. Rust formatting passed after additive-conflict module ordering. Full Windows backend and package checks are separately recorded by the release workflow; these frontend results do not imply backend success.

Build entry point: [Local Windows release workflow](https://github.com/Tomodad/cc-switch/actions/workflows/local-windows-release.yml). Its push trigger is limited to the dedicated integration branch; publication is gated on all validation and package checks.
