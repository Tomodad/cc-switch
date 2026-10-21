# Local v4.0.17 integration

Official baseline: `19798e2f92a4ff949a7647e07c33351bf597fa10`
(`origin` release tag `v4.0.17`, peeled commit).
Previous local business baseline: `eb478e7c728dd21521625c1acb59bba93b44ec67`.
Accepted 4.0.15 repair source: `61ccd03a`.

The release merges the official tag into the full retained local lineage.
Root/fork `main` is not used as the installed business baseline. New upstream
`main` changes after the release tag are excluded from the release candidate.

## Patch comparison

| Area | What v4.0.17 contains | Integration decision |
| --- | --- | --- |
| Native ChatGPT login lifecycle | Preserve login on detach, return a stashed login on enter/attach, avoid returning expired logins | Use official lifecycle logic; it complements the repair but does not replace built-in provider identity |
| Unified history and Dot identity | Existing shared `custom` bucket remains the upstream design | Retain the repair: official and opted-in API proxy routes use built-in `openai`; migrate known legacy shared history with backups |
| Backend-scoped web-search history | No equivalent portable-history adapter in the tag | Retain request, JSON and SSE projection and opted-in legacy history migration |
| Missing ToolSearch declarations | No equivalent missing-declaration shim | Retain scoped local gates and tests |
| Native Responses ToolSearch round trips | No equivalent complete native request/history/JSON/SSE restoration | Retain typed IDs, successful-attempt context and restoration |
| Dynamic schemas, namespace subsets | Official namespace foundations already present; no equivalent full local increment | Retain local schema aliases and loaded-subset behavior |
| Proxy model catalogs | Official custom context windows and aggregation compaction fixes | Keep official window/compaction changes together with local proxied tool capability profiles |
| Plaintext delegation | Official orphan-output handling is related but does not replace strict desktop envelope normalization | Retain bounded local adapter; preserve official orphan handling |
| Windows npm repair | Official fnm discovery expands CLI lookup, not failed sibling npm platform-package repair | Retain bounded local repair and official fnm discovery |
| Chat tool result ordering and context errors | Official contiguous tool-output and context-budget error improvements | Keep official implementation and local ToolSearch/media round-trip additions |
| Windows test fixtures | Symlink privilege fallback is not supplied by this tag | Retain test-only directory junction fallback and original assertions |
| Production UI packaging | Local read-only embedded asset gate | Retain and extend to both main window and new tray panel assets |

No complete retained compatibility patch was found to be fully superseded by
this release. Related official fixes above are partial overlaps, not grounds
to remove whole local features. One textual conflict in the vendor catalog
entry builder was resolved by retaining both the official window overrides
and local proxied ToolSearch declaration.

The user confirmed the 4.0.15 repair can continue this conversation through an
API route, shows Dot, and completes a Dot task. Those observations are not
v4.0.17 desktop acceptance. This candidate is built and archived for separate
installation and real-client testing; production settings are not changed.
