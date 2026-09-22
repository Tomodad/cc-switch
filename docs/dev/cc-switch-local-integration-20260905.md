# CC-Switch local integration and package plan (2026-09-05)

## Authorization and safety boundary

- Authorized: investigate, patch, merge, validate, build a Windows installer, and archive it under `E:\CC-Switch-Archive`.
- Not authorized: install/replace the running CC-Switch build, restart Codex, stop CC-Switch, alter live login/config/cache, or clean live databases.
- Do not publish a GitHub release, force-push, merge upstream PRs on behalf of maintainers, delete old branches, or overwrite historical archives.

## Fresh preflight pins

- Official `origin/main`: `db41d701879592b8eca938cbe5c5ac28dd732b9f` (`v3.20.1-28-gdb41d701`).
- PR #5265 initial preflight head: `2c427808e5f130f019bee50b539c8774ca9a6b11`; final repaired head integrated later: `5e6caf75`.
- PR #5799 head: `7e6203ddcd0ebde3d04c7a006f15625cf8a57ad0`.
- Installed CC-Switch: `3.20.0`, binary timestamp `2026-08-24T02:14:52+08:00`.
- Installed Codex CLI: `0.146.0`.
- Starting worktree: clean `main` at `0345fad6048eed65b3423bedc8ce5711320ddfc3`, 131 commits behind the pinned official main.

## Current upstream/PR evidence

- PR #5799 CI is green on Ubuntu, Windows, macOS, and Windows+WSL2; review approval is still required.
- PR #5265 was green on Windows and Windows+WSL2 but failed on Ubuntu and macOS in `codex_desktop_cache_sync_keeps_discovering_after_only_legacy_layout_updates`. The assertion treated root `Local Storage/leveldb` as legacy on every OS even though production intentionally treats it as active on macOS/Linux. Commit `8d22bc61` makes the expectation platform-aware and was pushed to the PR branch.
- The reasoning-picker acceptance criterion is behavioral: a visible model and a static `High` label do not prove that the effort selector works.

## Reasoning-picker diagnosis

- Live installed `C:\Users\Zhuyixiang\.codex\cc-switch-model-catalog.json` currently gives GPT-5.6 Sol/Terra/Luna and GPT-6 Astra only `none,high`, with default `high` and no `use_responses_lite` field.
- The same machine's official `models_cache.json` gives:
  - GPT-5.6 Sol: `low,medium,high,xhigh,max,ultra`, default `low`.
  - GPT-5.6 Terra: `low,medium,high,xhigh,max,ultra`, default `medium`.
  - GPT-5.6 Luna: `low,medium,high,xhigh,max`, default `medium`.
  - GPT-6 Astra: `low,medium,high,xhigh,max,ultra`, default `medium`.
- Codex Desktop 26.901.5280.0 reads `model/list`, retains `supportedReasoningEfforts` allowed by its `enabled-reasoning-efforts` setting, validates a saved effort against that list, and otherwise uses `defaultReasoningEffort`. With the installed custom catalog, filtering removes `none` and leaves only `high`, which explains a static High label without a meaningful effort choice.
- Behavioral RED: after adding the official Astra expectations to the catalog regression, Astra still produced `none,high` instead of six efforts.
- Commit `81eb1784` adds only the exact Astra fallback from the official local model catalog. It does not assign one universal list and does not override explicit per-model `reasoningLevels`.
- GREEN: Astra, Sol, Terra, and Luna catalog expectations pass; explicit user reasoning overrides remain higher priority.
- Outbound preservation regressions cover both HTTP request preparation and Responses WebSocket `response.create`: a selected `reasoning.effort = xhigh` survives to the upstream request body.
- Follow-up review RED found two external-catalog restore errors: the owned-path resolver received a file path instead of its parent directory, and verbatim restore trusted stale inline models before checking the prepared config's surviving external pointer. Commit `48decdb5` fixes both; the focused parent-directory and external-over-inline regressions pass.
- A final review found the managed-official takeover early return still bypassed prepared-config reconciliation. Commit `5e6caf75` routes that branch through the same external-catalog-aware helper; the managed takeover test and all 43 provider-service tests pass.

## Implementation sequence

1. Diagnose PR #5265 Linux/macOS failures and repair the PR branch if they are product failures.
2. Trace API-provider reasoning end to end: provider model metadata -> generated catalog -> Desktop Statsig/cache -> model capability UI -> persisted/default effort -> outbound `reasoning.effort`.
3. Record a focused RED for the exact picker/metadata failure. Prefer current official/provider model data; never assign one universal effort list to every model. Explicit user configuration and official catalog metadata remain higher priority than inferred fallback data.
4. Create a new `codex/` integration branch from the pinned official main.
5. Integrate the current clean #5265 and #5799 heads.
6. Re-evaluate and, only if still missing upstream, port Responses WebSocket commits `e552b4f8`, `bec3efe2`, `f5d2fdc5` and Windows updater repair `5534af53`.
7. Do not reintroduce superseded auth/history or old standalone reasoning patches such as `1cd23f71`.
8. Validate Rust formatting/Clippy, focused tests, serialized full Rust tests, frontend typecheck/format/unit tests/build, then isolated non-live runtime behavior where possible.
9. Build a Windows NSIS setup executable (MSI optional). If updater signing credentials are unavailable, use a temporary build override with `bundle.createUpdaterArtifacts=false`; do not edit the repository's permanent signing config.
10. Archive installer(s), `SHA256SUMS`, and a source/build/validation report in a unique version-date-SHA directory under `E:\CC-Switch-Archive`, then re-read sizes and hashes.

## Acceptance status

- [x] Fresh SHA/version/worktree preflight.
- [x] PR #5265 CI/root-path/restore/managed-takeover issues fixed and pushed through `5e6caf75`; fresh remote CI still needs maintainer approval.
- [x] Reasoning picker chain has catalog/UI/request behavioral RED/GREEN evidence.
- [x] Integration branch `codex/local-integration-3.20.1-20260905` created from pinned official main.
- [x] PR #5265/#5799, Responses WebSocket, and Windows updater repair integrated.
- [x] Focused and regression validation complete: reasoning catalog, HTTP/WS effort forwarding, ToolSearch, xAI, Moonshot/Kimi, WebSocket, and updater tests are green.
- [x] Rust formatting and Clippy `-D warnings` pass.
- [x] Final-tree serialized Rust library run: 2841 passed, 3 failed, 6 ignored. The failures are environmental: two Windows symlink privilege 1314 cases and one live proxy port collision 10048. Integration suites passed except `skill_sync`, where the first symlink privilege 1314 failure poisoned the test mutex and caused the second failure; the `support` target contains zero tests and is not counted as a pass.
- [x] Frontend typecheck and format check pass.
- [x] Clean serialized frontend run after removing the nested temporary worktree: 135 files and 1075 tests passed. The earlier contaminated run discovered the nested worktree and duplicated tests, so it is retained as invalid evidence rather than a product failure.
- [x] Renderer production build passes, with only existing dependency-age/chunk-size warnings.
- [ ] NSIS installer built and archived with verified hashes.
- [ ] Live UI picker/request acceptance (requires separate permission if restart/install/live-state change is necessary).
