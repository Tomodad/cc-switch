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

The original request below is an optional old-version causal baseline. The later preparation audit recommends testing the existing new candidate first if the user approves installation; reproducing on the old CC-Switch is not mandatory when the user's goal is simply to stop adding patches if the new combination works.

For either explicitly selected version, request one controlled, at-most-two-minute API-provider window, followed by restoration of OAuth. The coordinator sends this existing task one harmless delegated probe with model `gpt-5.6-sol` and effort `high`: reply with a unique marker only; use no tools, perform no implementation, and write no files. Do not resend the previous implementation instructions or create another task. Record the chosen runtime/model and route instead of assuming the coordinator's provider is inherited.

Before sending, enable a scoped capture only if it can collect the following without exposing credentials or recording unrelated requests. Any required live config/logging change needs explicit approval; any required app restart is a separate gate. If these boundaries cannot be observed, defer the probe rather than executing an uninstrumented replay.

| Boundary | Minimum retained fields |
| --- | --- |
| Desktop -> app-server | Timestamp, task/turn IDs, versions, input vs toolOutput shape, tool name/namespace, text length and digest, selected model/effort |
| App-server -> proxy/provider | Actual host/path and transport (HTTP/SSE/WS), correlation IDs, item types/order, role/name/namespace and presence of id/call_id, whether any matching call exists, marker presence, item length/digests, previous_response_id presence, model/effort/tool_choice/max-output limits |
| Before/after CC-Switch request conversion, if used | Same structural fields and hashes, selected adapter, removed/retyped items, marker still present; no full historical prompt dump |
| Upstream -> CC-Switch -> Desktop | Response ID/status, ordered event types, delta/text lengths at each boundary, function/custom call counts, completed/incomplete/failed marker, finish/incomplete reason, usage totals, close/error timing |

Compare response lengths on both sides before attributing loss. If upstream is empty already, retain provider/Codex attribution as pending. If upstream has text but downstream loses it, reproduce the captured structural case offline before changing production. No automatic retries of the delegated probe.

## Preparation audit: 2026-09-05, approximately 07:38 UTC / 15:38 Beijing

This follow-up performed only bounded reads and updated this report. It did not send a probe, install software, switch provider/login, change live logging/config/cache, stop a process, restart an app, modify AGENTS.md, or run a test/build matrix. Starting documentation HEAD was `a82cf10e83b574912d9e5c4c81e253efe609fa95`; production source still matches candidate `096af5b9`.

### Installed and candidate versions

| Item | Fresh evidence |
| --- | --- |
| Running CC-Switch | PID 23904, started 12:41:22 +08:00, installed path `C:\Users\Zhuyixiang\AppData\Local\CC Switch\cc-switch.exe`, version **3.20.0**, 34,374,656 bytes, modified 2026-08-24 02:14:52 +08:00 |
| Installed EXE SHA-256 | `8FC97DD7842137585C5F131E79529969A567C30AD56F6D12C1D0CF96E11A1562` |
| Current Desktop | Registered package **26.901.5280.0**; Desktop parent PID 9544 (process name `ChatGPT.exe`) |
| Actual Desktop app-server | PID **2236**, parent 9544, executable `C:\Users\Zhuyixiang\AppData\Local\OpenAI\Codex\bin\27d6a192e9c98618\codex.exe`. The matching current process's startup log reports **0.153.4**. Its PE ProductVersion is absent; the startup handshake is the version evidence |
| Other app-server | PID 30740 belongs to a separate npm CLI process tree. Do not mix its version or telemetry with Desktop PID 2236; it was not stopped |
| Current CC-Switch state | Codex card `codex-official / OpenAI Official`; `enabled=0`, `live_takeover_active=0`, logging enabled, port 15721. This is consistent with the user's retained official-login state; no credential contents were inspected |
| Candidate | `E:\CC-Switch-Archive\3.20.1-local-20260905-096af5b9\CC Switch_3.20.1_x64-setup.exe`, **10,275,059 bytes**, SHA-256 **8F54B17860ACB3D8E0F00987A78D619689C4AF3C01049595179D4CF0E7EB8EFF** |
| Provenance/signature | Recomputed hash matches SHA256SUMS; BUILD_VALIDATION.md names full HEAD `096af5b95c99d602230dbb6a7f0672638e32f639`; retained build log reports successful NSIS output. Authenticode reports **NotSigned**. This checks the existing recorded provenance without rebuilding |

The new package is still uninstalled. These are the same post-OAuth runtime versions previously observed, not the earlier failing Desktop 26.901.4073/app-server 0.153.1.

### Exact relevant installed-to-candidate source differences

Compared installed-build reference `0903818e` with candidate `096af5b9`, normalizing CRLF for comparisons:

- `proxy/providers/transform_codex_responses_namespace.rs` and `transform_codex_chat.rs` have no file diff. Their function-call-output handling, namespace rewrite, typed ToolSearch restoration, and ordinary-message preservation are already in the installed source reference.
- Production code before the test module in `responses_websocket.rs`, `streaming_codex_chat.rs`, and `streaming_codex_anthropic.rs` is identical. The WebSocket file's candidate changes are test coverage for reasoning.effort and a Clippy allowance; they do not alter its one-response.create-per-connection or frame restoration behavior.
- `proxy/forwarder.rs` adds the official xAI native request sanitizer after namespace flattening when the xAI gate applies. `proxy/handlers.rs` adds the xAI native response/SSE handler with whole-float argument normalization, routes the ToolSearch restore gate through it, and makes the Codex app-type gate explicit. The generic namespace-only helper keeps the existing false ToolSearch gate intact. No captured incident route proves the xAI branch ran.
- Catalog/provider integration adds Statsig visibility reconciliation, updated official provider/auth configuration behavior, per-model fallback metadata including Astra, and Responses Lite opt-out metadata. These can affect the end-to-end environment, but do not constitute an offline proof that delegation empty completion is fixed.
- Desktop's `toolOutput` input construction is outside CC-Switch and is not replaced by installing this package. Neither the unchanged transformer code nor the other new compatibility changes prove success or failure for an uncaptured target request.

Conclusion: **not proven fixed offline, and not proven ineffective**. The new candidate should be evaluated on the fixed current Desktop/runtime if the user approves it.

### Actual capture readiness (not all ready)

All existing paths below were opened read-only successfully during this audit. No new capture executable or paired traffic-observer output was found in the scoped repository diagnostic artifacts; the previous boundary table is a plan, not an installed collector.

| Needed observation | Existing usable method/output | Readiness and limitation |
| --- | --- | --- |
| Target task/turn and delegated input | Parse the existing target rollout at the path in the evidence ledger; use task_started, toolOutput-derived item, response_id and task_complete | **Ready for retained output-level evidence**: role/type/name/namespace, marker length/hash and final assistant text length; does not expose the serialized provider request |
| Desktop dispatch and versions | Current PID 9544 log listed in A2 and Windows process metadata | **Ready** for matching turn/start acknowledgment and active app-server version, not complete request bodies |
| HTTP provider route/status/usage | CC-Switch log `C:\Users\Zhuyixiang\.cc-switch\logs\cc-switch.log`; query `proxy_request_logs` in `C:\Users\Zhuyixiang\.cc-switch\cc-switch.db` with `data_source=proxy` and a matching response ID | **Conditionally available** for logged HTTP requests. Current card selection and data_source=codex_session alone do not prove the target's route |
| Runtime HTTP/WS route | Read `E:\Agent_file\.codex\logs_2.sqlite`, restricting to the actual Desktop process/task/turn when present | **Partial**: prior target turns were absent, even though other tasks' routes were retained. A future row cannot be promised |
| Request structure before/after conversion | Existing `log_prompt_cache_trace` in forwarder.rs produces hashes for instructions/tools/input/messages/body at Debug level | **Not ready for required paired structure**. It is one post-transform summary and is not called by the WS path; it lacks ordered per-item type/id fields and a before/after pair |
| Upstream/downstream events and text lengths | Rollout records only the final side; inspected HTTP/WS production logging does not retain paired event streams | **Missing**. No already-available setting identified in these paths can add the required paired observations |
| Derived sanitized test result | Current evidence report; a timestamped diagnostic artifact may be created when a test is actually authorized | **Plan-only output** for a future probe, not an existing successful capture |

Verified logging controls exist: `commands/settings.rs::get_log_config/set_log_config`, database `settings.log_config`, and dynamic `log::set_max_level`. No persisted log_config override is present, so the declared default is enabled **Info**. Raising it temporarily to Debug would write live DB state and produce the existing CacheTrace summary; it requires permission, does not require a logger restart by implementation, and **does not fill the missing upstream/downstream or WS boundary data**. Broad Trace or raw-body logging is not a substitute for a scoped, redacted observer.

To close the minimum **route** gap, arrange target-correlated HTTP/WS observation before the test. Reuse existing response-ID/transport telemetry if it demonstrably covers the target; otherwise a loopback observer or scoped diagnostic instrumentation must be prepared and validated offline first, then explicitly approved for temporary routing/logging changes. It must keep the same native transport, omit credentials/query secrets, and filter to the one marker/task. For **causal** diagnosis after another failure, observations on both sides of CC-Switch are additionally required. No such observer was written, connected, or claimed ready in this read-only follow-up. Temporary routing can interrupt traffic or alter timing, so its exact endpoint, restoration procedure and scope must be reviewed before activation.

### Recommended order and approval prerequisites

**Recommended next testing target: the already-built new candidate, once installation is separately approved. Do not require an old-version failure first. At this checkpoint the user should not yet independently install or switch to API: coordinated installation protection and reliable target-route observation are not in place.**

1. Finish the minimum target-route observation preparation above. Full paired causal capture can be deferred if the goal of the first test is only acceptance, but a visible selected API card is not sufficient route evidence. Do not label a replay with unknown routing as successful API-provider acceptance.
2. Request approval for one candidate installation, a consistent private local backup, exiting/relaunching CC-Switch, and one brief API-provider window with restoration of the original OAuth/provider state. No probe has been sent. Do not bundle a Codex restart or a logging/routing change into an unstated permission.
3. Before installation, save a consistent SQLite backup of CC-Switch data (SQLite backup API or a closed-app snapshot, not an unsafe live copy), its settings, and the affected Codex config/catalog files. Protect current login material as an opaque local backup only if authorized; never print it or place it in repository/PR/report output. Freeze the current Desktop 26.901.5280/app-server 0.153.4 and verify them again after installation. **Backup readiness is pending**: no new rollback snapshot was created in this read-only audit.
4. Exit current CC-Switch PID 23904 before replacing its executable, then install the exact verified NSIS once and relaunch it. Replacing CC-Switch does **not inherently require quitting Codex**. After provider/catalog change, check actual loaded route/metadata; if the existing target app-server retains stale state and cannot apply the change, request one separate Codex restart instead of assuming hot reload works.
5. Run one harmless marker-only delegated turn on the new CC-Switch with the fixed Desktop/app-server, the approved real API provider, and explicitly recorded Sol/high model/effort. This is acceptance of the current combination, not a replay of the old operational instructions. Use the existing task, no new task, no tools in the probe and no looped retries. Restore OAuth afterward.
6. **Pass**: target received the marker, produced the exact nonempty marker, completed, and correlated route evidence proves the actual approved API path. Report "not reproduced / acceptance passed in this combination" and stop further patching; historical root cause remains unproven. **Fail**: persist the turn/response IDs, leave the problem unresolved, and obtain missing paired capture before fixing. **Unknown route or stale versions**: evidence is inconclusive, not a pass; do not repeatedly send probes.

Rollback package was also verified: `E:\CC-Switch-Archive\3.20.0-origin-5ca9459d-installed-0241cd3a-merge-0903818e-20260823\CC-Switch-3.20.0-Origin5ca9459d-Merged-0903818e-x64-setup.exe`, 10,102,226 bytes, SHA-256 `B21B1904EE202ACD800A524FFAC943E7F7DF8843252BBD956324BCE438B1AF1E`, matching its SHA256SUMS.txt and build reference `0903818e`. A usable old installer exists; that alone is not a consistent configuration/DB rollback snapshot or proof of downgrade compatibility. If rollback is needed, stop CC-Switch and use the approved version-and-data recovery procedure; do not restore stale OAuth tokens over an account refreshed since backup without review.

Preparation status: **read-only audit complete; outcome-level evidence available; actual-route evidence conditional; complete causal capture not ready; installation/backup/provider-window permissions pending**. No conclusion that the new candidate solves the fault has been made.

## Repository guidance delivery

No repository AGENTS.md existed (checked tracked files and hidden/unignored instruction files). The global instruction file is outside this task's edit scope. Added `E:\cc-switch\AGENTS.md` with six concise rules: task-specific acceptance/permissions, worktree discovery isolation, focused-then-candidate validation, bounded review-before-packaging, output-directory build exclusion/cache reuse, and per-issue truthful status. The repository `.gitignore` already has a broad AGENTS.md pattern; stage this specifically authorized file with `git add -f -- AGENTS.md` without altering ignore rules or global files.

## Outcome

### Later executable-observer preparation

The subsequent authorized preparation implemented a default-off native Responses observer and offline verdict CLI; see `E:\cc-switch\docs\dev\route-probe-preparation.md` for exact commands, schemas, the prepared disabled config and focused RED/GREEN evidence. This supersedes the earlier **plan-only** observer status, not the historical root-cause conclusion.

The observer records actual selected provider and HTTP response receipt or separate local/upstream WS Upgrade and matched response.create send. An exact newest-input digest plus source/target/nonce, real upstream response-ID digest and target rollout completion must all correlate. A WS failure followed by a same-input HTTP/SSE completion is distinguishable from unrelated adjacent traffic. Headers not received on an HTTP timeout do not prove sending never started.

These are new diagnostic production-code changes. Installed 3.20.0 and archived `096af5b9` do not contain the hook. To use this observation method, request one later diagnostic build/deployment containing it; do not make the user install the old candidate first and then install another package. No such package, live activation, provider change or probe was performed in the preparation step.

- Goal A: **unresolved**. Existing evidence narrowed the failure, but cannot support a causal production fix; the controlled capture above is pending permission. Current OAuth and live software/config remain unchanged by this round.
- Goal B: **implemented and verified in `a82cf10e`**. AGENTS.md has six rules and was not changed in the later preparation steps. The existing installer remains tied to source HEAD `096af5b9`; executable diagnostic tools and their focused tests are a later, separately recorded source change.
