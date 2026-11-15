# Local v4.0.32 Dot candidate

Pinned installed parent: `119feb202ff6b55b05a9e295cb75f4d5b951c8d8` (local 4.0.27).
Pinned upstream: annotated `v4.0.32` tag `91e45e2c6ef4e9603525062e26f79ccaed543daf`, peeled commit `93d2381c8730aaf0078d44cd9c060dfa8133f859`.
Remote refs were checked before merging on 2026-11-15; main was unchanged. Only the dedicated candidate branch is updated. No force push, branch deletion, installation or private configuration access is authorized or performed.

## Identity contract

Dot preservation is the first priority: unified history keeps built-in `openai`, `openai_base_url` pointing only at the local proxy, and native authentication (`KeepNative` for API proxy routes). Third-party credentials and account-header/metadata isolation remain mandatory. Strict third-party direct mode cannot provide this contract. Dot additionally needs an eligible official account and a valid native login; cloud tests cannot establish client entitlement or rendering.

Pause official login uses a `custom` proxy route, so it conflicts with this identity. The UI disables the opposite option and explains the conflict. Backend settings saves reject both enabled together before any mutation. For pre-existing conflicting configuration, the effective pause getter returns false while unified history is enabled; built-in identity wins consistently in routing, official forwarding and model passthrough. No option silently disables unified history.

Only the local custom-to-openai v2 history migration runs after opt-in. Official openai-to-custom startup migration is excluded. Operation lock, source-directory/target-bound marker, separate backup generation, concurrent-file checks, state DB availability checks and rollout mtime protection are retained. The old v1 restore ledger is kept, and v2 migration is never reversed automatically.

## Retained patch audit

Compared the installed local patch against official .27 and .32 source, rather than inferring equivalence from PR status.

| Local patch | Official .32 coverage and disposition |
| --- | --- |
| Built-in identity / Dot / native auth | Official unified mode still selects custom; retain local openai contract and migration direction. New login-pause feature is mutually exclusive. Official direct-switch native-auth fallback remains when unified is off. |
| Portable search history | No equivalent module; retain request, JSON and SSE projection and backed-up v2 migration. |
| Plaintext delegation | No equivalent bounded envelope adapter; retain, alongside official encrypted-agent and opaque-state rejection handling. |
| ToolSearch / namespaces | Official namespace source is unchanged between .27 and .32; retain local typed names, schema aliases, loaded subset, collision and hosted-tool guards, request-scoped repair, successful-attempt context and JSON/SSE restoration. |
| Proxy catalogs | Official direct profiles are complementary; retain native Responses/Anthropic proxy profiles and native ToolSearch advertisement gates. |
| Known-model reasoning, Responses Lite, directory search | No complete equivalent replacement found; retain narrow local behavior and exclusions. |
| Windows npm repair | Official Windows repair still returns None; retain bounded sibling npm command and Windows command-status regression tests. |
| Embedded production UI and payload verification | No equivalent upstream publication gate; retain build/payload checks, exact Tauri marker comparison, checksums and source archive. |
| Windows test directory links | Retain junction fallback when symlink privilege is absent; tests retain canonical escape/cycle assertions. |
| Test portability fixes | Retain platform-gated real keychain constructor, slice cleanup and isolated settings fixtures; no test assertion is weakened. |

Upstream .28–.32 changes are additive: Windows stability, native Responses paths, HTTP-200 SSE opaque-state bounded retry, DB poison-lock recovery/transaction rollback, privacy filtering and startup history protections remain. Official updater endpoints/key remain unchanged. No Desktop cache edits or tool-choice broadening are introduced.

## Conflict decisions

Ten files conflicted. Migration keeps local v2 direction and adds official history split hints. Settings retains new official login/preserve-login resync and rollback plus local unified live reapply and migration completion event. Forwarder retains local pre-conversion projection/delegation and per-attempt ToolSearch contexts, followed by official privacy filtering and opaque SSE handling. Module declarations/exports are combined. Direct-provider plan retains official pause behavior only outside unified mode, built-in identity for unified API routes, and official auth fallback outside unified mode. Four locale conflicts retain local Dot descriptions. New official locales receive accurate English fallback for these local identity explanations; no custom-bucket description is left for unified mode.

Automatically merged handlers retain official Copilot stream preparation before local response restoration; native xAI fallback remains outbound-model aware. Official new privacy filtering runs after local adapters and again at the upstream exit. Retried attempts call the full preparation function and return their own restoration contexts.

## Validation and delivery

The fork-only `local-windows-release.yml` triggers on this candidate branch. It pins the upstream SHA, uses a new release tag `local-v4.0.32-dot-20261115`, refuses existing tags/releases, and gates publication on full frontend checks, Windows all-target Clippy with warnings denied, nextest, doctests, NSIS production build, embedded UI and extracted payload verification, exact byte comparison and SHA256 inventory. Tests include a new combined HTTP-200 SSE opaque-state retry case with portable history, plaintext delegation, ToolSearch provenance and OAuth-header isolation; both success and second rejection are tested to enforce a two-attempt bound. UI tests cover an old conflicting preference.

Actual CI result and artifact hashes must be read from the exact final commit's run and manifest; this document does not declare those checks passed in advance. Local Linux backend checking is blocked by missing GLib development libraries; Windows CI is authoritative. Cloud validation is not Windows GUI acceptance.

The initial complete Windows check on `c44c3631ffaa04ecbb85204d57c2f840ce5d8bb3` failed at strict Clippy (run 43682396180): local Windows npm repair test needed the new official no-op progress argument; a custom-bucket test helper needed `cfg(test)`; upstream WorkBuddy's Unix-only permission path variable was unused on Windows. These are fixed without relaxing assertions or warnings. Superseded preparation runs were canceled. Final run results must still be checked.

## Private backup, rollback and local acceptance

Database schema changes **24 to 26**. The official automatic backup logs a warning and continues on failure. Before first launch, close CC Switch and Codex and independently copy/verify their configuration, CC Switch database plus any WAL/SHM, and Codex history/native login into private storage. Verify an isolated restore, not just existence of a backup. Never upload private data to this repository or CI.

Keep the installed 4.0.27 binary/installer and schema-24 backup. Rollback requires closing the new app and restoring pre-upgrade data before launching the old program. The old executable must never open the schema-26 database. Post-upgrade changes are not automatically backported. Preserve old releases and all branches.

User's remaining local task: archive verified public artifacts and private backups separately under `E:\CC-Switch-Archive`; test with isolated data first, verify native Dot identity/rendering and a real Dot task, official/API switching, unified resume, portable web-search continuation, delegation, ToolSearch, login-pause conflict explanation and backup/restore. No automatic installation is performed.
