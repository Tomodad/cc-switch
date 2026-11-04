# Codex history and Dot compatibility

Baseline: local CC Switch 4.0.15, `eb478e7c728dd21521625c1acb59bba93b44ec67`.

The previous shared-history mode selected a provider named `custom`, including
for official OAuth sessions. Current Codex desktop capability checks require
the built-in `openai` provider for Dot. Naming a custom provider "OpenAI" does
not satisfy that check.

With unified history enabled, official login keeps the built-in provider and
API proxy routes use the built-in provider with a loopback `openai_base_url`.
The proxy selects the API upstream independently, replaces the incoming OAuth
authorization with its API credential, and removes official account headers
and internal request metadata. The native login remains available to desktop
account features. API strict direct mode is outside this compatibility path.

An opt-in v2 migration moves the known legacy `custom` history bucket to
`openai`. It backs up JSONL and SQLite state separately, preserves unrelated
providers, checks for concurrent file changes, honors the active profile, and
uses a new backup generation. Existing v1 backup ledgers remain intact.

Native web-search call handles can be scoped to their issuing backend. A
reproducible continuation failure returned `response protection is unavailable`
when replaying such handles to the official endpoint. Projecting only those
handles to readable historical activity messages allowed the same request to
complete. This preserves queries/actions and the surrounding conversation;
it cannot export hidden search context held only by the issuing backend.
Existing originals are retained in migration backups. New proxied API search
items are presented to Codex as portable history messages. The proxy leaves
reasoning ciphertext, ordinary messages, other tool calls and tool outputs
unchanged. Arbitrary upstreams, account changes and different model versions
still require their own acceptance tests.

The implementation covers request JSON, native Responses SSE and non-streaming
responses. SSE completion and usage accounting remain intact. Native desktop
Dot rendering and an actual Dot task in both modes require live client
acceptance; passing backend tests does not establish those results.

Keep the running installation and native login unchanged until the repair
candidate has passed checks and has a verified archive. This patch does not
include the upstream 4.0.17 upgrade.
