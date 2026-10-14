# Local v4.0.11 integration

Upstream: `2635dfffcee949b6056b936c992dfcdbdd940298` (v4.0.11).
Tested and packaged business source: `9d10ee9f5b93210ac41ce3e07eb0e10b2c6d7fdb`.
Business tree: `c361dd5a14be88ae17ca05afcf412ec2bfa708f2`.

Retains the scoped compatibility increments from `cdcdda8e`: missing ToolSearch shim, native Responses round trips, schema/namespace loaded subsets, proxy catalog capability profiles, strict plaintext delegation and bounded Windows npm platform-package repair. Reuses upstream registry and orphan-output handling. Includes the dormant Copilot override gate correction from PR8120 commit `440a55d3`. Explicit `tool_choice:none` remains restricted; no LevelDB/Statsig cache changes.

Offline frontend/release builds and 87 selected backend regressions passed. No full suite was repeated. User confirmed the independent trial could save a test provider and retain it after exit/restart, then installed the production package.

Production identifier: `com.ccswitch.desktop`. Trial identifier: `com.ccswitch.acceptance.v4011-20261014`. Production packaging restores the production identifier, so the trial EXE and installer payload are not byte-identical. The complete extracted NSIS payload equals the production release EXE after the UNK-to-NSS bundle marker substitution.

- NSIS installer SHA256: `145d99d833b9bff5af20cb67fdee5e34fcde117efcfa8bccb8c7b9221e6a78ed`
- Installed/payload SHA256: `b9f2d64a0d341aaeee6e6b005f8f476cca111e908aa892ba9e1f75446c1e6600`
- Trial EXE SHA256: `5503f8230708d956beff99bdf7365aac11cbeebe241ec1bfa447ec8ded63797d`

Installed version 4.0.11 and payload hash were independently verified after the user reported installation. No new model call, installer execution or production configuration edit was performed by the agent. The old integration report describes historical 4.0.9 validation, not this run.

Archive policy: retain the current v4.0.11 and previous v4.0.9 integration branches, main, and the open PR8120 source branch. Older integration branches require a verified remote archive tag and a verified local source snapshot before reference deletion.
