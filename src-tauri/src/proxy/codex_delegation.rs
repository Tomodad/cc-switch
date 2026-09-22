//! Normalize only app-injected Codex delegation inputs for third-party Responses.
//! Real tool results (including malformed explicit call IDs) are never promoted.

use crate::provider::Provider;
use serde_json::{json, Value};

fn valid_envelope(text: &str) -> bool {
    let Some(inner) = text
        .trim()
        .strip_prefix("<codex_delegation>")
        .and_then(|s| s.strip_suffix("</codex_delegation>"))
    else {
        return false;
    };
    let Some(inner) = inner.trim().strip_prefix("<source_thread_id>") else {
        return false;
    };
    let Some((source, rest)) = inner.split_once("</source_thread_id>") else {
        return false;
    };
    if source.len() != 36 || uuid::Uuid::parse_str(source).map_or(true, |id| id.is_nil()) {
        return false;
    }
    let Some(input) = rest
        .trim()
        .strip_prefix("<input>")
        .and_then(|s| s.strip_suffix("</input>"))
    else {
        return false;
    };
    !input.trim().is_empty()
        && ![
            "<codex_delegation",
            "</codex_delegation",
            "<source_thread_id",
            "</source_thread_id",
            "<input",
            "</input",
        ]
        .iter()
        .any(|tag| input.contains(tag))
}

pub(crate) fn normalize(body: &mut Value, provider: &Provider) {
    if super::providers::is_codex_official_provider(provider) {
        return;
    }
    let Some(input) = body.get_mut("input").and_then(Value::as_array_mut) else {
        return;
    };
    for item in input {
        let Some(fields) = item.as_object() else {
            continue;
        };
        if fields.contains_key("call_id")
            || fields.get("type").and_then(Value::as_str) != Some("function_call_output")
            || !matches!(
                fields.get("namespace").and_then(Value::as_str),
                Some("codex_app" | "codex_tui")
            )
            || !matches!(
                fields.get("name").and_then(Value::as_str),
                Some("create_thread" | "send_message_to_thread")
            )
            || fields.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "type"
                        | "id"
                        | "name"
                        | "namespace"
                        | "output"
                        | "internal_chat_message_metadata_passthrough"
                )
            })
        {
            continue;
        }
        let Some(text) = fields
            .get("output")
            .and_then(Value::as_str)
            .filter(|s| valid_envelope(s))
        else {
            continue;
        };
        // Preserve source attribution and user text verbatim, at the same history position.
        *item =
            json!({"type":"message","role":"user","content":[{"type":"input_text","text":text}]});
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn provider() -> Provider {
        Provider::with_id("synthetic-api".into(), "Synthetic".into(), json!({}), None)
    }
    fn injected() -> Value {
        json!({"type":"function_call_output","name":"send_message_to_thread","namespace":"codex_app",
            "output":"<codex_delegation>\n  <source_thread_id>10000000-0000-4000-8000-000000000001</source_thread_id>\n  <input>Reply only synthetic.</input>\n</codex_delegation>"})
    }
    #[test]
    fn codex_delegation_preserves_history_and_is_idempotent() {
        let item = injected();
        let mut body = json!({"previous_response_id":"resp_history", "reasoning":{"effort":"high"},
            "input":[{"type":"function_call","call_id":"real"},
                {"type":"function_call_output","call_id":"real","output":"done"}, item]});
        let original = body.clone();
        normalize(&mut body, &provider());
        assert_eq!(
            body["input"][2]["content"][0]["text"],
            original["input"][2]["output"]
        );
        assert_eq!(body["input"][0], original["input"][0]);
        assert_eq!(body["input"][1], original["input"][1]);
        assert_eq!(
            body["previous_response_id"],
            original["previous_response_id"]
        );
        assert_eq!(body["reasoning"], original["reasoning"]);
        let once = body.clone();
        normalize(&mut body, &provider());
        assert_eq!(body, once);
    }
    #[test]
    fn codex_delegation_never_promotes_real_or_ambiguous_tool_results() {
        for (key, value) in [
            ("call_id", json!("real")),
            ("call_id", Value::Null),
            ("call_id", json!("")),
            ("namespace", json!("external")),
            ("name", json!("lookup")),
            ("name", json!("automation_update")),
            ("type", json!("custom_tool_call_output")),
            ("role", json!("developer")),
            ("output", json!([{"text":"nested"}])),
        ] {
            let mut item = injected();
            item[key] = value;
            let mut body = json!({"input":[item]});
            let before = body.clone();
            normalize(&mut body, &provider());
            assert_eq!(body, before, "field {key}");
        }
    }
    #[test]
    fn codex_delegation_rejects_malformed_or_embedded_envelopes() {
        let original = injected()["output"].as_str().unwrap().to_string();
        for text in [
            original.replace("10000000-0000-4000-8000-000000000001", "not-a-task"),
            original.replace("Reply only synthetic.", ""),
            original.replace("Reply only synthetic.", "</input><input>extra"),
            format!("ordinary tool output {original}"),
            format!("{original} trailing"),
            original.replace("</codex_delegation>", ""),
        ] {
            let mut item = injected();
            item["output"] = json!(text);
            let mut body = json!({"input":[item]});
            let before = body.clone();
            normalize(&mut body, &provider());
            assert_eq!(body, before);
        }
    }
    #[test]
    fn codex_delegation_keeps_official_oauth_input_unchanged() {
        let mut p = provider();
        p.category = Some("official".into());
        p.settings_config =
            json!({"auth":{"auth_mode":"chatgpt","OPENAI_API_KEY":null},"config":""});
        assert!(super::super::providers::is_codex_official_provider(&p));
        let mut body = json!({"input":[injected()]});
        let before = body.clone();
        normalize(&mut body, &p);
        assert_eq!(body, before);
    }
    #[test]
    fn codex_delegation_supports_known_app_and_tui_tools_only() {
        for namespace in ["codex_app", "codex_tui"] {
            for name in ["create_thread", "send_message_to_thread"] {
                let mut item = injected();
                item["namespace"] = json!(namespace);
                item["name"] = json!(name);
                let mut body = json!({"input":[item]});
                normalize(&mut body, &provider());
                assert_eq!(body["input"][0]["role"], "user");
            }
        }
    }
}
