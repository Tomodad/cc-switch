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
        // Preserve source attribution and user text at the same history position.
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
        let mut body = json!({"previous_response_id":"resp_history", "reasoning":{"effort":"high"},
            "input":[{"type":"function_call","call_id":"real"},
                {"type":"function_call_output","call_id":"real","output":"done"}, injected()]});
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
            ("call_id", json!(0)),
            ("call_id", json!({})),
            ("namespace", json!("external")),
            ("name", json!("lookup")),
            ("name", json!("automation_update")),
            ("type", json!("custom_tool_call_output")),
            ("role", json!("developer")),
            ("unexpected_metadata", json!(true)),
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
            original.replace(
                "10000000-0000-4000-8000-000000000001",
                "00000000-0000-0000-0000-000000000000",
            ),
            original.replace("Reply only synthetic.", ""),
            original.replace("Reply only synthetic.", " \n\t "),
            original.replace("Reply only synthetic.", "</input><input>extra"),
            original.replace("Reply only synthetic.", "<codex_delegation>nested"),
            original.replace("Reply only synthetic.", "<source_thread_id>nested"),
            original.replace(
                "</source_thread_id>",
                "</source_thread_id><unexpected>extra</unexpected>",
            ),
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
    fn codex_delegation_is_not_opaque_state_rectification() {
        use super::super::opaque_state_rectifier::{rectify_opaque_state, OpaqueStateRejection};

        let mut body = json!({"input":[injected()]});
        let before = body.clone();
        let result = rectify_opaque_state(
            &mut body,
            OpaqueStateRejection {
                reasoning: true,
                compaction: true,
            },
        );
        assert!(!result.applied);
        assert_eq!(body, before);

        normalize(&mut body, &provider());
        assert_eq!(body["input"][0]["role"], "user");
        assert_eq!(
            body["input"][0]["content"][0]["text"],
            before["input"][0]["output"]
        );
    }

    #[test]
    fn codex_delegation_survives_chat_protocol_conversion() {
        let mut body = json!({"model":"synthetic-model", "input":[injected()]});
        let text = body["input"][0]["output"].clone();
        normalize(&mut body, &provider());
        let chat =
            super::super::providers::transform_codex_chat::responses_to_chat_completions(body)
                .expect("normalized delegation must be accepted by the Chat converter");
        assert_eq!(chat["messages"][0]["role"], "user");
        assert_eq!(chat["messages"][0]["content"], text);
    }

    #[test]
    fn codex_delegation_survives_anthropic_protocol_conversion() {
        let mut body = json!({"model":"synthetic-model", "input":[injected()]});
        let text = body["input"][0]["output"].clone();
        normalize(&mut body, &provider());
        let anthropic =
            super::super::providers::transform_codex_anthropic::responses_request_to_anthropic(
                body, 4096,
            )
            .expect("normalized delegation must be accepted by the Anthropic converter");
        assert_eq!(anthropic["messages"][0]["role"], "user");
        assert_eq!(
            anthropic["messages"][0]["content"],
            json!([{"type":"text", "text":text}])
        );
    }

    #[test]
    fn codex_delegation_composes_with_official_chat_orphan_outputs() {
        let mut body = json!({"model":"synthetic-model", "tool_choice":"none",
            "tools":[{"type":"function","name":"read","parameters":{"type":"object"}}],
            "input":[injected(), {"type":"function_call_output", "output":"ordinary receipt"}]});
        let text = body["input"][0]["output"].clone();
        normalize(&mut body, &provider());
        // Only the validated delegation is normalized here. The official Chat
        // converter still owns ordinary orphan outputs and its diagnostic marker.
        assert_eq!(body["input"][1]["type"], "function_call_output");
        let chat =
            super::super::providers::transform_codex_chat::responses_to_chat_completions(body)
                .unwrap();
        assert_eq!(chat["tool_choice"], "none");
        assert_eq!(chat["messages"][0]["content"], text);
        assert_eq!(chat["messages"][1]["role"], "user");
        assert_eq!(
            chat["messages"][1]["content"],
            "[tool_result without a matching tool_call]\nordinary receipt"
        );
    }

    #[test]
    fn codex_delegation_composes_with_official_xai_orphan_outputs() {
        let mut body = json!({"tool_choice":"none", "input":[injected(),
            {"type":"function_call_output", "output":"ordinary receipt"},
            {"type":"function_call_output", "call_id":"real", "output":"paired result"}]});
        let text = body["input"][0]["output"].clone();
        let paired = body["input"][2].clone();
        normalize(&mut body, &provider());
        assert!(super::super::providers::transform_codex_responses_xai_sanitize::rewrite_xai_call_id_less_tool_outputs(&mut body));
        assert_eq!(body["tool_choice"], "none");
        assert_eq!(body["input"][0]["content"][0]["text"], text);
        assert_eq!(body["input"][1]["role"], "user");
        assert_eq!(
            body["input"][1]["content"][0]["text"],
            "[tool_result without a matching tool_call]\nordinary receipt"
        );
        assert_eq!(body["input"][2], paired);
        let once = body.clone();
        normalize(&mut body, &provider());
        assert!(!super::super::providers::transform_codex_responses_xai_sanitize::rewrite_xai_call_id_less_tool_outputs(&mut body));
        assert_eq!(body, once);
    }

    #[test]
    fn codex_delegation_keeps_envelope_bytes_with_allowed_metadata() {
        let mut item = injected();
        item["id"] = json!("injected_message_1");
        item["internal_chat_message_metadata_passthrough"] = json!({"source":"app"});
        item["output"] = json!(format!(" \n{}\t ", item["output"].as_str().unwrap()));
        let text = item["output"].clone();
        let mut body = json!({"input":[item]});
        normalize(&mut body, &provider());
        assert_eq!(
            body["input"][0],
            json!({"type":"message", "role":"user", "content":[{"type":"input_text", "text":text}]})
        );
    }

    #[test]
    fn codex_delegation_requires_explicit_shape_and_top_level_input_array() {
        for missing in ["type", "namespace", "name", "output"] {
            let mut item = injected();
            item.as_object_mut().unwrap().remove(missing);
            let mut body = json!({"input":[item]});
            let before = body.clone();
            normalize(&mut body, &provider());
            assert_eq!(body, before, "missing field {missing}");
        }
        for mut body in [
            Value::Null,
            json!({}),
            json!({"input":injected()}),
            json!({"input":[null, "text", {"nested":injected()}]}),
        ] {
            let before = body.clone();
            normalize(&mut body, &provider());
            assert_eq!(body, before);
        }
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
