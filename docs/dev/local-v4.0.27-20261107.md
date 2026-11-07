# Local v4.0.27 integration

## Source and scope

Official baseline: v4.0.27, `26629ff49f0ca3c2ea9fee426ce8e911d52d2a85`.
Previous personalized baseline: v4.0.25, `e328c06ed685eb18caf9c9d7ec99fb14ad8dfbda`.
The complete personalized lineage is merged with the official release tag, not later upstream main.

## Audit of official coverage

Checked the official v4.0.25-to-v4.0.27 commit range and affected source against the prior personalized patch. On 2026-11-07 upstream PRs #8640 (built-in Codex identity and Dot) and #8641 (portable web-search history) remain open and unmerged. Neither is evidence of official replacement.

- Built-in identity, native authentication preservation and history migration: official direct-provider implementation and history migration have no changes in this range. Retain the local implementation.
- Portable web-search history: no equivalent official projection module or migration. Retain JSON/SSE and request projection.
- ToolSearch and native namespace restoration: official namespace module is unchanged. Retain request-scoped missing-declaration repair, typed identities, schema aliases, namespace merge, loaded subsets and successful-attempt restoration contexts.
- Native Responses: upstream extends xAI compatibility to Copilot Grok and stabilizes Copilot stream IDs. These are complementary, not replacements for local generic ToolSearch restoration. Preserve both pipelines.
- Model catalogs: upstream adds optional official Fast tiers and absolute native catalog paths. Neither replaces the local proxy tool profiles. Retain proxy profiles alongside these official changes.
- Plaintext delegation: upstream classifier-loop and encrypted-agent handling improvements do not replace the bounded local desktop-envelope adapter. Retain both.
- Windows npm package repair: official Windows codex_repair_command still returns None. New npm EEXIST hints and foreign Python pi.exe filtering are complementary. Retain bounded sibling npm repair.
- Embedded production UI verification, known-model reasoning fallback, Responses Lite and directory-search visibility remain local. No complete equivalence was established that would justify deleting them.

## Merge and validation requirements

Resolve additive module declarations, the new Copilot/native-Responses request entry point, outbound-model-aware xAI checks and upstream buffered SSE parsing without losing local ToolSearch or portable-history context. Strict Windows all-target Clippy, full frontend and backend suites, production NSIS build, embedded UI checks, exact payload comparison and release checksum verification gate publication. Passing source merge or cloud checks is not Windows desktop acceptance.

The database schema remains 24 from v4.0.25. Back up application configuration/database and Codex history privately before first launch. Upgrades from v4.0.17 still require schema 20-to-24 migration and a pre-upgrade backup for rollback. No private profiles, credentials or databases belong in public artifacts.

Do not restore private Desktop cache edits, change tool_choice none to auto, broaden allowed tools or remove hosted-ToolSearch/collision guards. Existing updater endpoints and public key remain unchanged. No automatic installation is performed.

After verified release publication retain public v4.0.27 and v4.0.25. Older assets and source history must remain recoverable unless separately authorized for permanent deletion.
