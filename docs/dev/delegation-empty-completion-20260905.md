# Delegation empty completion: bounded investigation and workflow update

## Scope and initial state

- Starting branch: `codex/local-integration-3.20.1-20260905`.
- Starting HEAD: `096af5b95c99d602230dbb6a7f0672638e32f639`; working tree initially clean.
- Goal A: explain the two API-provider delegated turns that ended without an assistant response; fix production code only if captured evidence supports a reproducible defect.
- Goal B: add concise repository-scoped workflow constraints to the applicable AGENTS.md.
- Preserve the existing catalog/Statsig/reasoning integration. No repeat upstream merge, package build, or live provider switch is part of read-only/documentation work.

## Acceptance and authority

| Goal | Required evidence | Validation | Initial status |
| --- | --- | --- | --- |
| A: empty completion | Original rollout shape, time-correlated proxy/request/response evidence, matching code path | Evidence-backed RED/GREEN if a defect can be reproduced; otherwise a bounded capture request and explicit unresolved status | Investigating |
| B: workflow guidance | Actual instruction hierarchy and existing repository guidance | Small scoped diff, UTF-8/no BOM, no duplicate or conflicting instructions | Pending |

Authorized: local reads, scoped source/doc changes, existing PR commit/push scope. Live login/provider changes, restart, install, stopping the active proxy, modifying live config/cache/DB, and global instruction edits require separate permission. Do not resend operational delegation instructions as a test.

## Steps and report checkpoints

1. Verify paths and instruction hierarchy; inspect the original failed turn structures without displaying prompts or credentials.
2. Correlate only those turn windows with retained CC-Switch/Codex logs and the actually installed source baseline. Report findings before considering a reproduction.
3. Stop widening searches when existing evidence is insufficient; define exact missing fields and one harmless controlled reproduction for approval.
4. Update repository AGENTS.md with six general workflow constraints, validate and path-stage only this round's documents/source.
5. Report A and B independently, including remaining live gates and delivered commit. Packaging applies only to an accepted production fix.

## Evidence ledger

- `C:\Users\Zhuyixiang\.codex` is a symbolic link to `E:\Agent_file\.codex`; these are one source, not independent captures.
- Failed-turn rollout: `E:\Agent_file\.codex\sessions\2026\09\04\rollout-2026-09-04T23-43-28-01a06d16-d7ca-7fb0-8889-af23b969ad61.jsonl`.
- Historical timestamps supplied for investigation: Beijing 2026-09-05 05:04:44 and 05:05:24; must be checked against raw UTC timestamps before correlation.

### A1. Original input and completion records (verified)

| Event | First failure | Second failure | Successful retry |
| --- | --- | --- | --- |
| UTC start | 2026-09-04 21:04:44.353 | 2026-09-04 21:05:24.069 | 2026-09-05 04:46:10.007 |
| Turn ID | `01a06e3c-f737-7720-87e0-3b3d8c93bd24` | `01a06e3d-925d-79c3-b6e8-ff76005b94ff` | `01a06fe3-6a4d-7d51-a80d-06a0991b11a4` |
| Delegation record line | 2764 | 2776 | 2791 |
| Body characters | 2271 | 382 | 1582 |
| Input shape | `function_call_output`, name `send_message_to_thread`, namespace `codex_app` | Same | Same |
| `call_id` key | Absent (not an explicit JSON null) | Absent | Absent |
| Local model/effort | `gpt-5.6-sol / high` | Same | Same |
| First output | Empty assistant `output_text` after reasoning item | Same | Nonempty custom `exec` tool call |
| Completion | 21:04:57.088 UTC; `last_agent_message=null` | 21:05:29.992 UTC; `last_agent_message=null` | Actual implementation followed |

The failure durations are 12.735 s and 5.923 s. Empty assistant records are at lines 2769 and 2781, each with `phase=final_answer` and one zero-character `output_text`. No intervening implementation tool calls occurred in either failed turn. These are persisted empty completions, not just a missing rendered chat bubble.

The input records contain `type,id,name,namespace,output,internal_chat_message_metadata_passthrough`. Their item IDs start with `fco_`. They are distinct from the source task's tool-call IDs. A missing `call_id` in this internal record is not sufficient evidence of a malformed request on the external Responses API.

Usage records at lines 2770 and 2782 identify responses:

- `resp_057e55c7508234c7016a9b3271ae1487d0a5d62fed7b5d4227`: input 217690, output 175, reasoning output 169.
- `resp_057e55c7508234c7016a9b32983e4087d0ae4a459c3ed33177`: input 217867, output 21, reasoning output 15.

The successful retry used a different prompt and input-token count (195544). No encrypted reasoning content, authentication material, or raw prompt bodies were exported into this report.

### A2. Desktop transport and version evidence (verified)

Failure log:
`C:\Users\Zhuyixiang\AppData\Local\Packages\OpenAI.Codex_2p2nqsd0c76g0\LocalCache\Local\Codex\Logs\2026\09\04\codex-desktop-9ac0322e-244d-4905-86ab-5851fc565d25-17128-t0-i1-205711-0.log`.

- Line 5 identifies Desktop **26.901.4073.0**; line 17 reports app-server **0.153.1**. The shell-installed CLI version 0.146.0 from the prior report is not this Desktop app-server's version.
- Lines 534 and 617 acknowledge the target task's `turn/start`, with `errorCode=null` and 16/13 ms dispatch duration.
- Lines 535 and 618 show successful source-tool delivery. Lines 564 and 631 show the target's reasoning items completed. Neither proves the upstream text was nonempty.

Successful-retry log:
`C:\Users\Zhuyixiang\AppData\Local\Packages\OpenAI.Codex_2p2nqsd0c76g0\LocalCache\Local\Codex\Logs\2026\09\05\codex-desktop-19a9ef09-2503-48ff-9ac3-f03e4f5c1f77-9544-t0-i1-044412-0.log`.

- Lines 5 and 17 identify Desktop **26.901.5280.0 / app-server 0.153.4**. Line 279 acknowledges the 04:46:10 UTC retry.
- OAuth success is therefore an uncontrolled comparison: runtime versions, login state, prompt, and accumulated context differ. It does not establish a fix or blame API authentication.

Read-only inspection of the available 26.901.5280.0 `app.asar`, `webview/assets/app-initial-ffce11d82782.js`, finds `sYn` selecting `input:[]` plus `toolOutput:{name,namespace,output}` for app-servers supporting `turnToolOutput` (threshold 0.151.0-alpha.4). This corroborates the dedicated delegation input, rather than a normal response to a matching tool call. The historical 26.901.4073.0 ASAR is no longer present; the current source is not represented as a byte-identical copy of it.

### A3. What the proxy evidence can and cannot establish

- CC-Switch's installed file remains version 3.20.0, timestamp 2026-08-24 02:14:52 +08:00. The prior installed-build source reference is `0903818e6049858deef680aeb380f76299c640cc`; current integrated source is `096af5b9`. Source-path analysis used the former where behavior could differ.
- `C:\Users\Zhuyixiang\.cc-switch\logs\cc-switch.log`, lines 26609-26616, shows nearby WebSocket handshake timeouts and two native Responses **Astra** HTTP requests. These are not matched to the target **Sol** turns. Rust transport logs link nearby traffic to other task IDs.
- Read-only SQLite (`mode=ro`, `PRAGMA query_only=ON`) on `C:\Users\Zhuyixiang\.cc-switch\cc-switch.db` finds the two Sol records only as `data_source=codex_session`, provider `_codex_session`, latency 0. The source importer assigns `status_code=200` (installed `src-tauri/src/services/session_usage_codex.rs:1488`). Those rows are usage imported from rollout, not proof of observed HTTP 200 responses.
- The installed WebSocket path logs errors but does not call the HTTP usage logger. Absence of matching `data_source=proxy` entries cannot establish that the target bypassed CC-Switch.
- The retained `logs_2.sqlite` has no target-thread rows in the bounded failure window (21:00-21:10 UTC), nor in the successful retry window. It contains other tasks' transport telemetry. No matching target request bodies or raw upstream SSE/WebSocket frames were recovered.
- Installed native history rewrite handles `tool_search_call` and `tool_search_output`; namespace input rewrite handles function calls. The restoration function returns without editing ordinary `message` output items. The Chat adapter does consume `function_call_output.call_id`, but no captured target request establishes that this adapter was selected. A speculative fixture against that code would not prove this incident's cause.

### A4. Diagnosis boundary

Ruled out as sufficient explanations: missing delegation text, failure to start the target turn, UI-only loss of an otherwise persisted response, or the internal `call_id` key's absence alone. Source delivery and target empty output are verified. The exact provider/transport, serialized wire input, and before/after response content for the target remain unverified. Neither a CC-Switch defect nor an upstream/Codex limitation is established.

No production change, live capture, behavioral repair RED/GREEN, rerun of the previous validation matrix, or new installer was attempted. Historical empty completions are incident evidence; they are not a newly reproduced production test failure.

## Minimal reproduction request (pending permission; not executed)

Request one controlled, at-most-two-minute API-provider window while retaining the currently installed software versions, followed by restoration of OAuth. The coordinator sends this existing task one harmless delegated probe with model `gpt-5.6-sol` and effort `high`: reply with a unique marker only; use no tools, perform no implementation, and write no files. Do not resend the previous implementation instructions or create another task. Record the chosen runtime/model and route instead of assuming the coordinator's provider is inherited.

Before sending, enable a scoped capture only if it can collect the following without exposing credentials or recording unrelated requests. Any required live config/logging change needs explicit approval; any required app restart is a separate gate. If these boundaries cannot be observed, defer the probe rather than executing an uninstrumented replay.

| Boundary | Minimum retained fields |
| --- | --- |
| Desktop -> app-server | Timestamp, task/turn IDs, versions, input vs toolOutput shape, tool name/namespace, text length and digest, selected model/effort |
| App-server -> proxy/provider | Actual host/path and transport (HTTP/SSE/WS), correlation IDs, item types/order, role/name/namespace and presence of id/call_id, whether any matching call exists, marker presence, item length/digests, previous_response_id presence, model/effort/tool_choice/max-output limits |
| Before/after CC-Switch request conversion, if used | Same structural fields and hashes, selected adapter, removed/retyped items, marker still present; no full historical prompt dump |
| Upstream -> CC-Switch -> Desktop | Response ID/status, ordered event types, delta/text lengths at each boundary, function/custom call counts, completed/incomplete/failed marker, finish/incomplete reason, usage totals, close/error timing |

Compare response lengths on both sides before attributing loss. If upstream is empty already, retain provider/Codex attribution as pending. If upstream has text but downstream loses it, reproduce the captured structural case offline before changing production. No automatic retries of the delegated probe.

## Repository guidance delivery

No repository AGENTS.md existed (checked tracked files and hidden/unignored instruction files). The global instruction file is outside this task's edit scope. Added `E:\cc-switch\AGENTS.md` with six concise rules: task-specific acceptance/permissions, worktree discovery isolation, focused-then-candidate validation, bounded review-before-packaging, output-directory build exclusion/cache reuse, and per-issue truthful status. The repository `.gitignore` already has a broad AGENTS.md pattern; stage this specifically authorized file with `git add -f -- AGENTS.md` without altering ignore rules or global files.

## Outcome

- Goal A: **unresolved**. Existing evidence narrowed the failure, but cannot support a causal production fix; the controlled capture above is pending permission. Current OAuth and live software/config remain unchanged by this round.
- Goal B: **implemented and verified**. Both documents decode as strict UTF-8 without BOM or replacement characters; AGENTS.md has six rules and no conflict markers. Only these two documentation files are included in this round's commit. No application tests/build are required for these documentation-only changes; the existing installer remains tied to source HEAD `096af5b9`.
