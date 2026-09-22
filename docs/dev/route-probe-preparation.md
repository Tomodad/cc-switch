# One-shot route observation preparation

Starting source: `f7dbb497` (production identical to archived candidate `096af5b9`).

Diagnostic production commit: `655a82f833b38e2a5a10875e15512c4d9dc70c21` (four Rust files). Scripts, schema and documentation are delivered in the following separate commit. No existing installer was changed.

## Scope

Prepare an opt-in, marker-scoped CC-Switch diagnostic hook plus an offline result tool. A proxy-side observation is joined with the target rollout; neither adjacent traffic nor a local WebSocket Upgrade alone can pass. The hook observes selected provider/upstream transport and terminal response identity. It does not change protocol payloads or store raw prompts, headers, credentials, or response text.

## Work and acceptance

1. Behavioral RED for selection and offline verdicts using clearly synthetic, sanitized inputs.
2. Minimal hook at actual HTTP forwarding and WS connect/send/receive points; output disabled by default, expires within 120 seconds, strict marker/source/target and selected-provider matching, bounded attempts/events/file size.
3. Offline parser joins nonce/run/task/turn/response IDs and requires exact marker response. Cover HTTP, WS, WS-to-SSE fallback, unrelated traffic, missing correlation, timeout, and unexpected sensitive fields.
4. Run focused tool/module checks only. Keep diagnostic production changes in a separate commit from scripts/docs; do not build an installer or touch live state.
5. Deliver executable prepare/arm/stop/report commands, schema and version applicability. Offline readiness is distinct from deployed-hook readiness and approved real reproduction.

No live capture, probe, provider/config/logging/DB change, process stop, install or restart is authorized in this preparation step. Activation and diagnostic-binary deployment require later approval.

## Executable artifacts

- E:\cc-switch\scripts\diagnostics\route_probe.mjs: prepare/arm/stop/report CLI and conservative verdict parser.
- E:\cc-switch\scripts\diagnostics\route_probe_checks.mjs: explicitly invoked Node tests; the filename intentionally does not enter Vitest discovery.
- E:\cc-switch\scripts\diagnostics\route_probe.schema.json: config, metadata-event and result schemas.
- E:\cc-switch\src-tauri\src\proxy\route_probe.rs: default-off observer. Forwarder and responses_websocket call it at native Responses boundaries.

The supported first probe is Codex native Responses over HTTP/JSON, HTTP/SSE or upstream WS. Chat/Anthropic conversion routes are outside this bounded observer and cannot receive PASS through it. No system proxy, MITM certificate, network listener or protocol retry is added.

## Correlation and verdict

The generated marker, source task UUID and target task UUID must occur together in the newest input item's exact delegation envelope. Supported input forms are function_call_output.output string and a user message containing one text content item. Old markers earlier in history are ignored. A configured expected provider ID must match the provider actually selected by CC-Switch; built-in Codex Official is excluded.

An attempt_id is the observer's generated request/connection-attempt UUID, not a claimed upstream x-request-id. The event also includes emitter PID, source/target UUIDs, nonce digest, sanitized host/path and actual upstream transport. Request payloads, provider IDs, model IDs and response IDs are represented by selected counts or hashes; raw provider credentials and ordinary response text are never written.

The parser joins the captured newest-item digest to the received delegation in the target rollout, verifies its task/turn, then joins the actual upstream response-ID digest to that turn's token_usage_record. It also requires an exact marker at the target's completed assistant output and zero tool calls. Source/target names, nearby timestamps, IPs and imported usage alone cannot substitute for this join.

| Verdict | Meaning |
| --- | --- |
| PASS | One uniquely correlated native upstream completion contains the exact marker; the matching target turn completed with the same marker and no tool calls. Provider ID and actual route/transport checks passed |
| FAIL | Fully correlated, observed terminal response failed/incompleted or did not produce the requested exact marker; or the target did something other than the marker-only instruction |
| INCONCLUSIVE | No matching hook observations (including direct bypass, undeployed hook or unsupported input/adapter), missing response ID, incomplete/expired/oversized/corrupted evidence, unexpected redirect, compressed/unobservable body, missing terminal output snapshot, or ambiguous/mixed process/turn correlation |

PASS means this software/provider/model combination passed the one harmless acceptance probe. It does not identify the historical cause or prove every future delegated instruction will work.

### Transport evidence is explicit

- selected: the nonce-bearing outgoing native request and actual chosen provider/route matched; it is NOT evidence that network sending completed.
- downstream_upgrade (WS only): recorded retrospectively after receiving the matched response.create on the established downstream WebSocket. It proves the local connection upgraded, not the upstream.
- upstream_upgrade (WS only): connect_async completed its upstream handshake.
- request_sent (WS only): sending/flushing that matched response.create succeeded. These three states are separate; a local Upgrade alone cannot pass.
- response_headers (HTTP only): the upstream response has been received. This establishes a response-stage observation, NOT the time sending began. Absence of headers on failure/timeout cannot establish that no bytes were sent. The result uses http_response_received and ws_send_recorded, not one ambiguous send flag.
- terminal: whitelisted terminal status, response-ID digest, output presence, text byte count, exact-marker boolean and item/frame counts. Reasoning items are ignored for marker equality; tool calls are not.
- closed/send_failed: local observation/callback lifecycle or fixed WS failure phase. closed is not proof of a protocol success; raw error strings are omitted.

WS-to-SSE fallback is true only when the same configured source/target/nonce and exact received-input digest identify a WS failure before the subsequent HTTP/SSE attempt, whose response ID also joins the target rollout. Time adjacency alone cannot set fallback. A normal WS close followed by HTTP does not, by itself, prove a failed WS fallback.

## Data minimization and hard limits

Activation requires BOTH the process-scoped CC_SWITCH_ROUTE_PROBE_CONFIG environment variable and a valid enabled config. With no variable there is no config read or output file. The default generated config is disabled. No existing logging setting or live database is changed; Debug/Trace is unnecessary.

- Config <= 4 KiB, known keys only; marker uses a fresh 128-bit nonce; source/target/run IDs must be UUIDs.
- Max 120 seconds, four matched attempts, 64 metadata events, 4 KiB per event and 64 KiB per events.jsonl.
- Max 64 KiB framing buffer/frame, 512 observed text frames and 1 MiB observed text per attempt. Exceeding observation limits never changes forwarded bytes and cannot yield PASS.
- File is created with create_new, so an existing capture is not overwritten. Unknown URL paths are hashed; userinfo/query/fragment are removed before writing. IDs are hashed to prevent an unexpected credential-shaped ID from being echoed.
- The sink checks enabled config and deadline before writing; stop immediately disables future observation in existing attempts. The forwarding connection itself is not stopped.
- One run per diagnostic process; generating another run after use requires a separately approved process relaunch. There are no automatic probes or retry loops.
- Raw history/SSE/WS data is parsed only transiently in bounded memory. Only constructed allowlisted metadata is passed to disk writes. Parser exceptions never print raw JSON, headers or file contents.

## Prepared, still disabled

A real-source/target configuration was prepared without arming it:

    E:\cc-switch\release\route-probe\97b951d4-2d67-4623-9a66-8954e3faf98f\config.json

Its source is the coordinator task and its target is this existing task. Expected provider ID is the previously observed sub2api-1776106473309; verify that remains the intended API card before future activation. The sibling prompt.txt contains the one marker-only instruction. Neither was sent to an agent. This directory is under the repository's already-ignored release directory and must not be committed.

The exact output locations, once genuinely activated, are:

    E:\cc-switch\release\route-probe\97b951d4-2d67-4623-9a66-8954e3faf98f\events.jsonl
    E:\cc-switch\release\route-probe\97b951d4-2d67-4623-9a66-8954e3faf98f\result.json

events.jsonl does not exist merely because preparation succeeded. Result generation reads the target rollout without changing it. Config/probe output cleanup is manual, confined to that exact run directory after retaining any required sanitized report. Do not delete live logs, databases or other run directories.

## Commands and future permissions

Offline checks (safe, synthetic temp files only; no live requests):

    & 'C:\Program Files\nodejs\node.exe' --test E:\cc-switch\scripts\diagnostics\route_probe_checks.mjs
    & 'C:\Program Files\nodejs\node.exe' --check E:\cc-switch\scripts\diagnostics\route_probe.mjs

To prepare a new disabled run, if a later run is explicitly needed:

    & 'C:\Program Files\nodejs\node.exe' E:\cc-switch\scripts\diagnostics\route_probe.mjs prepare --source 01a06e37-1da8-7703-8d2e-819dfb1de5fd --target 01a06d16-d7ca-7fb0-8889-af23b969ad61 --provider sub2api-1776106473309 --dir E:\cc-switch\release\route-probe

The following are FUTURE ACTIVATION instructions, not actions taken during preparation:

1. Obtain permission to build/deploy a diagnostic executable from the new source, back up affected live state, exit/relaunch CC-Switch, and perform the one temporary API-provider window. Verify the binary/PID/version and intended provider. Pin current Codex Desktop/app-server; do not substitute an unrelated npm app-server. A Codex restart, if needed for stale routing, is a separate permission.
2. The installed 3.20.0 and archived 096af5b9 NSIS do NOT contain this hook. Do not install 096af5b9 just to try enabling it. Prefer one later diagnostic candidate containing the existing fixes plus this hook; no installer was built in this round.
3. In the approved launch PowerShell only, set the diagnostic variable and launch the approved new executable after the previous CC-Switch has exited. Its single-instance handling otherwise could leave the old process active:

       $routeProbeConfig = 'E:\cc-switch\release\route-probe\97b951d4-2d67-4623-9a66-8954e3faf98f\config.json'
       $env:CC_SWITCH_ROUTE_PROBE_CONFIG = $routeProbeConfig
       Start-Process -FilePath '<approved-diagnostic-cc-switch.exe>' -WindowStyle Hidden

4. Once the approved actual API route is ready, arm for 120 seconds. Arming sets only this dedicated file; the CLI never sends a request:

       & 'C:\Program Files\nodejs\node.exe' E:\cc-switch\scripts\diagnostics\route_probe.mjs arm --config $routeProbeConfig

5. Coordinator sends prompt.txt once to the existing target task using send_message_to_thread, with the agreed model/effort. No implementation instructions and no tool calls in the probe. Do not resend automatically.
6. Stop observation and restore the approved original provider/OAuth state. Do not forcibly stop a running turn or restart Codex without separate permission:

       & 'C:\Program Files\nodejs\node.exe' E:\cc-switch\scripts\diagnostics\route_probe.mjs stop --config $routeProbeConfig
       Remove-Item Env:\CC_SWITCH_ROUTE_PROBE_CONFIG -ErrorAction SilentlyContinue

   Removing the shell variable alone does not change an already-running child's environment; stop disabling config is what revokes an existing attempt.

7. Produce the minimized result:

       & 'C:\Program Files\nodejs\node.exe' E:\cc-switch\scripts\diagnostics\route_probe.mjs report --config $routeProbeConfig --rollout E:\Agent_file\.codex\sessions\2026\09\04\rollout-2026-09-04T23-43-28-01a06d16-d7ca-7fb0-8889-af23b969ad61.jsonl

If PASS, accept the fixed observed combination and stop adding patches. If FAIL, retain the minimized result/IDs and investigate only the indicated failure. If INCONCLUSIVE, address the precise missing boundary; do not silently convert it into a pass or loop probes. Deeper paired-content capture requires a new approval and is not part of this observer.

## Validation ledger

- Synthetic Rust selector RED: one expected match failed before implementation; GREEN: six module tests cover selection, unrelated/default-disabled/expired/wrong-provider suppression, safe WS summaries, byte-identical fragmented HTTP/SSE, revocation/budgets and URL/config secret rejection.
- Synthetic Node verdict RED: four behavioral failures (HTTP/WS/fallback PASS and routed empty-output FAIL); initial GREEN 10/10, expanded to 23 checks including schema key coverage and final-answer selection after stricter correlation and HTTP response-receipt semantics.
- Scoped Rust results: route_probe 6 passed, responses_websocket 7 passed, native_followup 4 passed. The last disabled-fast-path tightening reran the first two affected groups; the history transformer was unchanged. Production-library Clippy with -D warnings, Rust format and Node syntax/style checks passed. Final Node checks: 23 passed, zero failed (including the final-answer guard, already added before scope closure).
- Disabled-state readback: the prepared config is enabled=false, expires_ms=0; events.jsonl is absent and this preparation process has no CC_SWITCH_ROUTE_PROBE_CONFIG opt-in. Test CLI arm/stop output came only from synthetic mkdtemp directories, removed by their tests, not from the prepared real run.
- Offline readiness: implemented and focused-validated; live capability is NOT deployed. No real probe or capture, installation, live setting mutation, new NSIS build or historical root-cause claim.
