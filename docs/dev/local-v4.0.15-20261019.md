# Local v4.0.15 integration

Official baseline: `79e754ab1b6ed50da57dae1d0faecab4b3b52f1c` (v4.0.15).
Retained local branch: `89d906329de6f23b9cb2c5d5a65590f8b84ad601`, whose business source is `9d10ee9f5b93210ac41ce3e07eb0e10b2c6d7fdb`.

The integration retains missing ToolSearch declarations with scoped gates, native Responses request/JSON/SSE round trips, dynamic schema and namespace loaded subsets, proxy catalog capability profiles, strict plaintext delegation, and bounded Windows npm platform-package repair. Official malformed-history recovery, DeepSeek replay normalization, stream termination, automatic approval/subagent catalogs, and stricter repair eligibility remain in place.

The two local Windows `ToolInstallation` test fixtures now set the official `exited_with_error` field. A failed npm launcher may qualify for repair; a healthy launcher does not. The official timeout-does-not-uninstall behavior is preserved.

Two existing directory-link tests initially failed with Windows error 1314. Their Windows-only, test-only fixture now falls back to a temporary directory junction when symbolic-link privilege is unavailable. The same canonical-path escape and cycle assertions still run; privileges are not changed and production code is unaffected.

No Desktop private LevelDB/Statsig cache behavior, tool-choice permission widening, or removed WebSocket extensions are restored. Production identity remains `com.ccswitch.desktop`.

Validation logs, results, source provenance, installer payload verification, and checksums are delivered with the versioned archive. Prior v4.0.11 test results do not count toward this candidate. The user will perform installation separately; building and archiving do not imply a completed production installer transaction or real-client acceptance.
