//! Codex `namespace` tool flattening for native Responses upstreams.
//!
//! Codex 0.142+ declares its plugin/MCP tools with a private Responses
//! extension shape — `{"type":"namespace","name":"mcp__x__","tools":[…]}` plus
//! `tool_search` — that the OpenAI ChatGPT backend understands but strict
//! third-party gateways (e.g. xAI's `api.x.ai/v1/responses`) reject with
//! `422 unknown variant "namespace"`. cc-switch's Chat/Anthropic transforms
//! already unwrap these, but the *native* Responses passthrough sends them
//! verbatim.
//!
//! This module implements the request-side flatten + response-side restore for
//! that native path, mirroring the proven design of sub2api
//! (`pkg/apicompat/responses_namespace.go`):
//!
//! - **Request**: lift every `namespace` child into a top-level `function` tool
//!   whose name is the deterministic flat name `<namespace>__<child>` (with the
//!   same sha256 truncation used by the Chat path, so both layers agree), then
//!   rewrite namespace-qualified `function_call` items in the replayed `input`
//!   history and exact named tool choices to the flat name. Reject selectors
//!   that cannot be represented without relaxing their restriction.
//! - **Response**: restore the flat `function_call` names back to
//!   `{name, namespace}` and convert the synthetic `function_call` named
//!   `tool_search` into the native client-executed `tool_search_call` expected
//!   by Codex (streaming and non-streaming).
//!
//! Flatten and restore both derive their name map from the *same* request tools
//! via [`flatten_namespace_tool_name`], so the forwarder (flatten) and the
//! response handler (restore) stay consistent without threading state between
//! them.

use std::collections::HashMap;

use bytes::Bytes;
use futures::stream::{Stream, StreamExt};
use serde_json::{json, Value};

use super::transform_codex_chat::{flatten_namespace_tool_name, tool_search_arguments_from_value};
use crate::proxy::error::ProxyError;
use crate::proxy::json_canonical::canonical_json_string;
use crate::proxy::sse::{append_utf8_safe, strip_sse_field, take_sse_block};

/// Reverse map entry: a flattened tool name resolves back to its original
/// namespace and bare child name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NamespacedName {
    pub namespace: String,
    pub name: String,
}

/// Return-path provenance captured from the exact provider request preparation.
/// Never infer ToolSearch ownership by recognizing an application's function schema.
#[derive(Debug, Clone, Default)]
pub(crate) struct NativeResponsesToolContext {
    pub namespace_restore_map: HashMap<String, NamespacedName>,
    pub restore_tool_search: bool,
}

pub(crate) fn tool_is_deferred(tool: &Value) -> bool {
    ["defer_loading", "deferLoading"]
        .iter()
        .any(|key| tool.get(key).and_then(Value::as_bool) == Some(true))
}

/// Explicit hosted/server search has no client-side transport equivalent.
/// Reject it rather than silently changing the executor or manufacturing a call ID.
pub(crate) fn ensure_client_tool_search(body: &Value) -> Result<(), ProxyError> {
    let declared_server = request_declared_tools(body).iter().any(|tool| {
        tool.get("type").and_then(Value::as_str) == Some("tool_search")
            && tool.get("execution").and_then(Value::as_str) == Some("server")
    });
    let server_history = body
        .get("input")
        .and_then(Value::as_array)
        .is_some_and(|items| {
            items.iter().any(|item| {
                matches!(
                    item.get("type").and_then(Value::as_str),
                    Some("tool_search_call" | "tool_search_output")
                ) && item.get("execution").and_then(Value::as_str) == Some("server")
            })
        });
    if declared_server || server_history {
        return Err(ProxyError::TransformError("server-executed ToolSearch cannot be converted to a client-executed compatibility function".to_string()));
    }
    Ok(())
}

/// Prepare a provider-gated native Responses request atomically. The caller must
/// exclude official routes, compaction, and saved tool/choice overrides.
pub(crate) fn prepare_native_responses_request(
    body: &mut Value,
    allow_missing_tool_search: bool,
) -> Result<NativeResponsesToolContext, ProxyError> {
    if super::codex_compaction::is_compaction_request(body) {
        return Ok(NativeResponsesToolContext::default());
    }
    ensure_client_tool_search(body)?;
    let mut prepared = body.clone();
    // Preserve the official additional_tools carrier semantics, merging namespace
    // children before dropping carriers so repeated namespaces do not lose tools.
    promote_additional_tool_declarations(&mut prepared)?;
    if allow_missing_tool_search {
        super::transform_codex_chat::inject_missing_tool_search(&mut prepared);
    }
    let can_replace = prepared.get("tool_choice").is_none_or(|choice| {
        choice.as_str() == Some("auto") || choice == &json!({"type": "tool_search"})
    });
    let context = super::transform_codex_chat::build_codex_tool_context_from_request(&prepared);
    let owns_search_name = context
        .lookup_chat_name("tool_search")
        .is_some_and(|spec| spec.kind == super::transform_codex_chat::CodexToolKind::ToolSearch);
    let mut restore_tool_search = false;
    let application_owns_search = super::transform_codex_chat::contains_application_tool_search_name(
        prepared.get("tools").unwrap_or(&Value::Null),
    )
        || prepared
            .get("input")
            .is_some_and(super::transform_codex_chat::contains_application_tool_search_name);
    if can_replace && owns_search_name && !application_owns_search {
        if let Some(tools) = prepared.get_mut("tools").and_then(Value::as_array_mut) {
            for tool in tools {
                if tool.get("type").and_then(Value::as_str) == Some("tool_search") {
                    let mut transport =
                        super::transform_codex_chat::tool_search_responses_function_tool();
                    for key in ["description", "parameters", "strict"] {
                        if let Some(value) = tool.get(key) {
                            transport[key] = value.clone();
                        }
                    }
                    *tool = transport;
                    restore_tool_search = true;
                }
            }
        }
        if restore_tool_search
            && prepared.get("tool_choice") == Some(&json!({"type": "tool_search"}))
        {
            prepared["tool_choice"] = json!({"type": "function", "name": "tool_search"});
        }
    }
    normalize_tool_search_history(&mut prepared)?;
    // Capture namespaces only after declaration merging, honoring original identity
    // precedence and deferred restrictions, before flattening loses the namespace.
    let namespace_restore_map = namespace_restore_map(&prepared);
    flatten_request_namespaces(&mut prepared)?;
    *body = prepared;
    Ok(NativeResponsesToolContext {
        namespace_restore_map,
        restore_tool_search,
    })
}

/// Build the flat-name → `{namespace, name}` restore map from a Codex Responses
/// request body. Used by the response handler to invert the request-side
/// flatten; derives names exactly as [`flatten_request_namespaces`] does.
pub(crate) fn namespace_restore_map(request_body: &Value) -> HashMap<String, NamespacedName> {
    let mut map = HashMap::new();
    // Legacy xAI callers need only the declarations actually sent upstream.
    // Managed native preparation promotes approved carriers before calling this.
    for tool in request_body
        .get("tools")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if tool.get("type").and_then(Value::as_str) != Some("namespace") || tool_is_deferred(tool) {
            continue;
        }
        let Some(namespace) = tool.get("name").and_then(Value::as_str) else {
            continue;
        };
        let namespace = namespace.trim();
        if namespace.is_empty() {
            continue;
        }
        for child in namespace_children(tool) {
            if child.get("type").and_then(Value::as_str) != Some("function")
                || tool_is_deferred(&child)
            {
                continue;
            }
            let Some(name) = child.get("name").and_then(Value::as_str) else {
                continue;
            };
            let name = name.trim();
            if name.is_empty() {
                continue;
            }
            let flat = flatten_namespace_tool_name(namespace, name);
            map.entry(flat).or_insert_with(|| NamespacedName {
                namespace: namespace.to_string(),
                name: name.to_string(),
            });
        }
    }
    map
}

/// The search result carrier is a protocol declaration, never arbitrary result data.
/// Older Codex clients omit execution/status; explicit non-client/non-completed
/// values do not declare a loaded set. A call ID is required for the client exchange.
pub(crate) fn is_loaded_tool_search_output(item: &Value) -> bool {
    item.get("type").and_then(Value::as_str) == Some("tool_search_output")
        && item
            .get("call_id")
            .and_then(Value::as_str)
            .is_some_and(|id| !id.trim().is_empty())
        && item
            .get("execution")
            .is_none_or(|execution| execution.as_str() == Some("client"))
        && item
            .get("status")
            .is_none_or(|status| status.as_str() == Some("completed"))
}

/// This helper is called only for the loaded subset from a protocol search output
/// (or an eligibility-only clone); it must never make the whole inventory callable.
pub(crate) fn normalize_loaded_response_tool(tool: &mut Value) {
    normalize_response_function_tool(tool);
    if let Some(obj) = tool.as_object_mut() {
        obj.remove("deferLoading");
        obj.remove("defer_loading");
        for key in ["tools", "children"] {
            if let Some(children) = obj.get_mut(key).and_then(Value::as_array_mut) {
                for child in children {
                    normalize_loaded_response_tool(child);
                }
            }
        }
    }
}

fn promote_additional_tool_declarations(body: &mut Value) -> Result<(), ProxyError> {
    let Some(obj) = body.as_object_mut() else {
        return Err(ProxyError::TransformError(
            "native Responses request body must be an object".to_string(),
        ));
    };
    if obj.get("tools").is_some_and(|tools| !tools.is_array()) {
        return Err(ProxyError::TransformError(
            "native Responses tools must be an array".to_string(),
        ));
    }
    let Some(input) = obj.get("input").and_then(Value::as_array) else {
        return Ok(());
    };
    if !input
        .iter()
        .any(|item| item.get("type").and_then(Value::as_str) == Some("additional_tools"))
    {
        return Ok(());
    }
    let mut tools = obj
        .get("tools")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut retained = Vec::with_capacity(input.len());
    for item in input {
        if item.get("type").and_then(Value::as_str) == Some("additional_tools") {
            if let Some(carried) = item.get("tools").and_then(Value::as_array) {
                for tool in carried {
                    merge_declared_tool(&mut tools, tool.clone(), false);
                }
            }
        } else {
            retained.push(item.clone());
        }
    }
    obj.insert("input".to_string(), json!(retained));
    if !tools.is_empty() {
        obj.insert("tools".to_string(), json!(tools));
    }
    Ok(())
}

pub(crate) fn request_declared_tools(request_body: &Value) -> Vec<Value> {
    let mut tools = request_body
        .get("tools")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if let Some(items) = request_body.get("input").and_then(Value::as_array) {
        // Explicit request declarations take priority over discovered schemas even
        // when the carriers are interleaved differently in conversation history.
        for item in items
            .iter()
            .filter(|item| item.get("type").and_then(Value::as_str) == Some("additional_tools"))
        {
            if let Some(carried) = item.get("tools").and_then(Value::as_array) {
                for tool in carried {
                    merge_declared_tool(&mut tools, tool.clone(), false);
                }
            }
        }
        if request_body.get("tool_choice").and_then(Value::as_str) != Some("none") {
            for item in items
                .iter()
                .filter(|item| is_loaded_tool_search_output(item))
            {
                if let Some(carried) = item.get("tools").and_then(Value::as_array) {
                    for tool in carried {
                        merge_declared_tool(&mut tools, tool.clone(), true);
                    }
                }
            }
        }
    }
    tools
}

fn merge_declared_tool(tools: &mut Vec<Value>, mut incoming: Value, loaded: bool) -> bool {
    if loaded {
        normalize_loaded_response_tool(&mut incoming);
    }
    let identity = response_tool_identity(&incoming);
    if let Some(existing) = tools
        .iter_mut()
        .find(|tool| identity.is_some() && response_tool_identity(tool) == identity)
    {
        let before = existing.clone();
        if incoming.get("type").and_then(Value::as_str) == Some("namespace") {
            let inherited_deferred = loaded && tool_is_deferred(existing);
            let children = namespace_children(&incoming);
            if let Some(obj) = existing.as_object_mut() {
                let key = if obj.get("tools").is_some_and(Value::is_array) {
                    "tools"
                } else if obj.get("children").is_some_and(Value::is_array) {
                    "children"
                } else {
                    "tools"
                };
                if !obj.get(key).is_some_and(Value::is_array) {
                    obj.insert(key.to_string(), json!([]));
                }
                if inherited_deferred {
                    obj.remove("defer_loading");
                    obj.remove("deferLoading");
                }
                if let Some(existing_children) = obj.get_mut(key).and_then(Value::as_array_mut) {
                    if inherited_deferred {
                        for child in existing_children.iter_mut() {
                            if let Some(child) = child.as_object_mut() {
                                child.insert("defer_loading".to_string(), json!(true));
                            }
                        }
                    }
                    for child in children {
                        merge_declared_tool(existing_children, child, loaded);
                    }
                }
            }
        } else if loaded {
            // Keep the original definition, only mark this exact identity loaded.
            normalize_loaded_response_tool(existing);
        }
        return *existing != before;
    }
    if !tools.contains(&incoming) {
        tools.push(incoming);
        true
    } else {
        false
    }
}

fn normalize_tool_search_history(body: &mut Value) -> Result<bool, ProxyError> {
    let mut discovered = Vec::new();
    let mut changed = false;
    if let Some(input) = body.get_mut("input") {
        changed |= rewrite_tool_search_history_items(input, &mut discovered)?;
    }
    if discovered.is_empty() || body.get("tool_choice").and_then(Value::as_str) == Some("none") {
        return Ok(changed);
    }

    let Some(obj) = body.as_object_mut() else {
        return Err(ProxyError::TransformError(
            "native Responses request body must be an object".to_string(),
        ));
    };
    let tools = obj
        .entry("tools".to_string())
        .or_insert_with(|| Value::Array(Vec::new()));
    if tools.is_null() {
        *tools = Value::Array(Vec::new());
    }
    let Some(tools) = tools.as_array_mut() else {
        return Err(ProxyError::TransformError(
            "native Responses tools must be an array".to_string(),
        ));
    };

    for tool in discovered {
        changed |= merge_declared_tool(tools, tool, true);
    }

    Ok(changed)
}

fn rewrite_tool_search_history_items(
    value: &mut Value,
    discovered: &mut Vec<Value>,
) -> Result<bool, ProxyError> {
    match value {
        Value::Array(items) => {
            let mut changed = false;
            for item in items {
                if item.is_object() {
                    changed |= rewrite_tool_search_history_items(item, discovered)?;
                }
            }
            Ok(changed)
        }
        Value::Object(_) => {
            let loaded_output = is_loaded_tool_search_output(value);
            let obj = value.as_object_mut().unwrap();
            match obj.get("type").and_then(Value::as_str) {
                Some("tool_search_call") => {
                    let call_id = obj
                        .get("call_id")
                        .and_then(Value::as_str)
                        .filter(|value| !value.is_empty())
                        .ok_or_else(|| {
                            ProxyError::TransformError(
                                "tool_search_call is missing call_id".to_string(),
                            )
                        })?
                        .to_string();
                    let arguments = match obj.get("arguments") {
                        Some(Value::String(value)) => value.clone(),
                        Some(value) => canonical_json_string(value),
                        None => "{}".to_string(),
                    };
                    *value = json!({
                        "type": "function_call",
                        "name": "tool_search",
                        "call_id": call_id,
                        "arguments": arguments
                    });
                    Ok(true)
                }
                Some("tool_search_output") => {
                    let call_id = obj
                        .get("call_id")
                        .and_then(Value::as_str)
                        .filter(|value| !value.is_empty())
                        .ok_or_else(|| {
                            ProxyError::TransformError(
                                "tool_search_output is missing call_id".to_string(),
                            )
                        })?
                        .to_string();
                    if loaded_output {
                        if let Some(tools) = obj.get("tools").and_then(Value::as_array) {
                            discovered.extend(tools.iter().cloned());
                        }
                    }
                    let output = canonical_json_string(value);
                    *value = json!({
                        "type": "function_call_output",
                        "call_id": call_id,
                        "output": output
                    });
                    Ok(true)
                }
                _ => Ok(false),
            }
        }
        _ => Ok(false),
    }
}

pub(crate) fn normalize_response_function_tool(tool: &mut Value) {
    let Some(obj) = tool.as_object_mut() else {
        return;
    };
    if obj.get("type").and_then(Value::as_str) == Some("namespace") {
        for key in ["tools", "children"] {
            if let Some(children) = obj.get_mut(key).and_then(Value::as_array_mut) {
                for child in children {
                    normalize_response_function_tool(child);
                }
            }
        }
        return;
    }
    if obj.get("type").and_then(Value::as_str) != Some("function") {
        return;
    }
    if let Some(function) = obj.get_mut("function").and_then(Value::as_object_mut) {
        if !function.contains_key("parameters") {
            if let Some(schema) = function
                .get("inputSchema")
                .or_else(|| function.get("input_schema"))
                .cloned()
            {
                function.insert("parameters".to_string(), schema);
            }
        }
        function.remove("inputSchema");
        function.remove("input_schema");
    }
    if !obj.contains_key("parameters") {
        if let Some(schema) = obj
            .get("inputSchema")
            .or_else(|| obj.get("input_schema"))
            .cloned()
        {
            obj.insert("parameters".to_string(), schema);
        }
    }
    obj.remove("inputSchema");
    obj.remove("input_schema");
    // Preserve inventory loading state. Only normalize_loaded_response_tool,
    // used for the protocol-declared loaded subset, removes true markers.
    for key in ["deferLoading", "defer_loading"] {
        if obj.get(key).and_then(Value::as_bool) == Some(false) {
            obj.remove(key);
        }
    }
}

fn response_tool_identity(tool: &Value) -> Option<String> {
    let kind = tool.get("type").and_then(Value::as_str)?;
    let name = tool
        .get("name")
        .or_else(|| tool.get("function").and_then(|value| value.get("name")))
        .and_then(Value::as_str)?;
    Some(format!("{kind}:{name}"))
}

/// Flatten Codex `namespace` tool declarations in a native Responses request
/// body into top-level `function` tools, rewrite namespace-qualified calls in
/// the `input` history, and preserve exact named tool selection.
///
/// Returns `Ok(true)` when the body was rewritten. Returns a `TransformError`
/// when two distinct namespace children (or a child and a top-level tool)
/// collapse to the same flat name — the upstream could not disambiguate them,
/// so failing loudly beats silently dropping a tool (matches sub2api).
pub(crate) fn flatten_request_namespaces(body: &mut Value) -> Result<bool, ProxyError> {
    flatten_request_namespaces_with_search_history(body, true)
}

/// Existing xAI/GrokBuild compatibility, deliberately without the managed Codex
/// ToolSearch history rewrite or loaded-definition promotion.
pub(crate) fn flatten_request_namespaces_only(body: &mut Value) -> Result<bool, ProxyError> {
    flatten_request_namespaces_with_search_history(body, false)
}

fn flatten_request_namespaces_with_search_history(
    body: &mut Value,
    search_history: bool,
) -> Result<bool, ProxyError> {
    let namespace_selection = body.pointer("/tool_choice/type").and_then(Value::as_str)
        == Some("namespace")
        || body
            .pointer("/tool_choice/tools")
            .and_then(Value::as_array)
            .is_some_and(|choices| {
                choices
                    .iter()
                    .any(|choice| choice.get("type").and_then(Value::as_str) == Some("namespace"))
            });
    if namespace_selection {
        return Err(ProxyError::TransformError(
            "native Responses cannot flatten a namespace-wide tool_choice without broadening its restriction".to_string(),
        ));
    }
    let mut changed = if search_history {
        normalize_tool_search_history(body)?
    } else {
        false
    };
    if let Some(tools) = body.get_mut("tools").and_then(Value::as_array_mut) {
        for tool in tools.iter_mut() {
            let previous = tool.clone();
            normalize_response_function_tool(tool);
            changed |= *tool != previous;
        }
        let previous_len = tools.len();
        tools.retain(|tool| !tool_is_deferred(tool));
        changed |= previous_len != tools.len();
    }
    let Some(tools) = body.get("tools").and_then(Value::as_array) else {
        return Ok(changed);
    };
    if !tools
        .iter()
        .any(|tool| tool.get("type").and_then(Value::as_str) == Some("namespace"))
    {
        return Ok(changed);
    }

    // Names already occupied by top-level function/custom tools; a namespace
    // child flattening onto one of these is an unrecoverable collision.
    let mut top_level = std::collections::HashSet::new();
    for tool in tools {
        let typ = tool.get("type").and_then(Value::as_str).unwrap_or("");
        if typ == "function" || typ == "custom" {
            if let Some(name) = tool.get("name").and_then(Value::as_str) {
                let name = name.trim();
                if !name.is_empty() {
                    top_level.insert(name.to_string());
                }
            }
        }
    }

    // Validate flat-name uniqueness before mutating anything.
    let mut owners: HashMap<String, NamespacedName> = HashMap::new();
    for tool in tools {
        if tool.get("type").and_then(Value::as_str) != Some("namespace") {
            continue;
        }
        let Some(namespace) = tool.get("name").and_then(Value::as_str).map(str::trim) else {
            continue;
        };
        if namespace.is_empty() {
            continue;
        }
        for child in namespace_children(tool) {
            if child.get("type").and_then(Value::as_str) != Some("function")
                || tool_is_deferred(&child)
            {
                continue;
            }
            let Some(name) = child.get("name").and_then(Value::as_str).map(str::trim) else {
                continue;
            };
            if name.is_empty() {
                continue;
            }
            let flat = flatten_namespace_tool_name(namespace, name);
            if top_level.contains(&flat) {
                return Err(ProxyError::TransformError(format!(
                    "namespace tool {namespace:?}/{name:?} flattens to {flat:?} which \
                     collides with a top-level tool of the same name; rename one of them"
                )));
            }
            let entry = NamespacedName {
                namespace: namespace.to_string(),
                name: name.to_string(),
            };
            if let Some(prev) = owners.get(&flat) {
                if *prev != entry {
                    return Err(ProxyError::TransformError(format!(
                        "namespace tools {:?}/{:?} and {namespace:?}/{name:?} both flatten to \
                         {flat:?}; rename one of them",
                        prev.namespace, prev.name
                    )));
                }
            } else {
                owners.insert(flat, entry);
            }
        }
    }

    // Rebuild the tools array with namespace children lifted to top level.
    let tools = body
        .get("tools")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut flattened: Vec<Value> = Vec::with_capacity(tools.len());
    let mut seen_flat = std::collections::HashSet::new();
    for mut tool in tools {
        if tool.get("type").and_then(Value::as_str) != Some("namespace") {
            normalize_response_function_tool(&mut tool);
            flattened.push(tool);
            continue;
        }
        let Some(namespace) = tool.get("name").and_then(Value::as_str).map(str::trim) else {
            continue;
        };
        for child in namespace_children(&tool) {
            if child.get("type").and_then(Value::as_str) != Some("function")
                || tool_is_deferred(&child)
            {
                continue;
            }
            let Some(name) = child.get("name").and_then(Value::as_str).map(str::trim) else {
                continue;
            };
            if name.is_empty() {
                continue;
            }
            let flat = flatten_namespace_tool_name(namespace, name);
            if !seen_flat.insert(flat.clone()) {
                continue;
            }
            let mut lifted = child.clone();
            if let Some(obj) = lifted.as_object_mut() {
                obj.insert("name".to_string(), json!(flat));
            }
            normalize_response_function_tool(&mut lifted);
            flattened.push(lifted);
        }
    }
    body["tools"] = json!(flattened);

    // Rewrite namespace-qualified function_call items in the replayed history.
    if let Some(input) = body.get_mut("input") {
        rewrite_namespace_qualified_input_items(input, &owners);
    }

    // Exact named selection can be transported without broadening its scope.
    if let Some(choice) = body.get_mut("tool_choice") {
        rewrite_namespace_qualified_call(choice, &owners);
        if choice.get("type").and_then(Value::as_str) == Some("allowed_tools") {
            if let Some(choices) = choice.get_mut("tools").and_then(Value::as_array_mut) {
                for choice in choices {
                    rewrite_namespace_qualified_call(choice, &owners);
                }
            }
        }
    }

    Ok(true)
}

/// Restore native Responses function-call identities. Namespace restoration
/// uses the request-derived map; tool-search restoration is enabled only on the
/// provider-gated third-party compatibility path.
pub(crate) fn restore_response_tool_calls(
    value: &mut Value,
    map: &HashMap<String, NamespacedName>,
    restore_tool_search: bool,
) -> bool {
    if map.is_empty() && !restore_tool_search {
        return false;
    }
    restore_response_output_items(value, map, restore_tool_search, &mut HashMap::new())
}

/// Restore a single parsed SSE event (e.g. `response.output_item.added` /
/// `.done` carrying a `function_call`). Returns whether anything changed.
pub(crate) fn restore_sse_event_tool_calls(
    event: &mut Value,
    map: &HashMap<String, NamespacedName>,
    restore_tool_search: bool,
    tool_search_item_ids: &mut HashMap<String, String>,
) -> bool {
    if map.is_empty() && !restore_tool_search {
        return false;
    }
    let event_type = event.get("type").and_then(Value::as_str).unwrap_or("");
    let mut changed = false;

    if matches!(
        event_type,
        "response.output_item.added" | "response.output_item.done"
    ) {
        if let Some(item) = event.get_mut("item") {
            changed |= restore_output_item(item, map, restore_tool_search, tool_search_item_ids);
        }
    } else if matches!(
        event_type,
        "response.created"
            | "response.queued"
            | "response.in_progress"
            | "response.completed"
            | "response.incomplete"
            | "response.failed"
    ) {
        if let Some(response) = event.get_mut("response") {
            changed |= restore_response_output_items(
                response,
                map,
                restore_tool_search,
                tool_search_item_ids,
            );
        }
    }

    let restored_item_id = matches!(
        event.get("type").and_then(Value::as_str),
        Some("response.function_call_arguments.delta" | "response.function_call_arguments.done")
    )
    .then(|| event.get("item_id"))
    .flatten()
    .and_then(Value::as_str)
    .and_then(|item_id| tool_search_item_ids.get(item_id))
    .cloned();
    if let Some(restored_item_id) = restored_item_id {
        event["item_id"] = json!(restored_item_id);
        changed = true;
    }

    changed
}

fn namespace_children(tool: &Value) -> Vec<Value> {
    tool.get("tools")
        .or_else(|| tool.get("children"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn rewrite_namespace_qualified_input_items(
    input: &mut Value,
    owners: &HashMap<String, NamespacedName>,
) {
    let Some(items) = input.as_array_mut() else {
        return;
    };
    for item in items {
        if item.get("type").and_then(Value::as_str) == Some("function_call") {
            rewrite_namespace_qualified_call(item, owners);
        }
    }
}

fn rewrite_namespace_qualified_call(
    item: &mut Value,
    owners: &HashMap<String, NamespacedName>,
) -> bool {
    let Some(obj) = item.as_object_mut() else {
        return false;
    };
    let namespace = obj
        .get("namespace")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("")
        .to_string();
    let name = obj
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("")
        .to_string();
    if namespace.is_empty() || name.is_empty() {
        return false;
    }
    let flat = flatten_namespace_tool_name(&namespace, &name);
    match owners.get(&flat) {
        Some(entry) if entry.namespace == namespace && entry.name == name => {
            obj.insert("name".to_string(), json!(flat));
            obj.remove("namespace");
            true
        }
        _ => false,
    }
}

fn typed_tool_search_item_id(obj: &serde_json::Map<String, Value>) -> Option<String> {
    if let Some(item_id) = obj
        .get("id")
        .and_then(Value::as_str)
        .filter(|item_id| item_id.starts_with("tsc_") && item_id.len() > 4)
    {
        return Some(item_id.to_string());
    }

    obj.get("call_id")
        .and_then(Value::as_str)
        .filter(|call_id| !call_id.is_empty())
        .map(|call_id| format!("tsc_{call_id}"))
}

fn restore_response_output_items(
    response: &mut Value,
    map: &HashMap<String, NamespacedName>,
    restore_tool_search: bool,
    tool_search_item_ids: &mut HashMap<String, String>,
) -> bool {
    let Some(output) = response.get_mut("output").and_then(Value::as_array_mut) else {
        return false;
    };
    let mut changed = false;
    for item in output {
        changed |= restore_output_item(item, map, restore_tool_search, tool_search_item_ids);
    }
    changed
}

fn restore_output_item(
    item: &mut Value,
    map: &HashMap<String, NamespacedName>,
    restore_tool_search: bool,
    tool_search_item_ids: &mut HashMap<String, String>,
) -> bool {
    let Some(obj) = item.as_object_mut() else {
        return false;
    };
    if obj.get("type").and_then(Value::as_str) != Some("function_call") {
        return false;
    }

    let is_tool_search = restore_tool_search
        && obj.get("namespace").is_none()
        && obj.get("name").and_then(Value::as_str) == Some("tool_search");
    if is_tool_search {
        let previous_item_id = obj.get("id").and_then(Value::as_str).map(str::to_string);
        let item_id = typed_tool_search_item_id(obj);
        let arguments = tool_search_arguments_from_value(obj.get("arguments"));
        obj.insert("type".to_string(), json!("tool_search_call"));
        obj.insert("execution".to_string(), json!("client"));
        obj.insert("arguments".to_string(), arguments);
        obj.remove("name");
        if let Some(item_id) = item_id {
            if previous_item_id.as_deref() != Some(item_id.as_str()) {
                if let Some(previous_item_id) = previous_item_id {
                    tool_search_item_ids.insert(previous_item_id, item_id.clone());
                }
                obj.insert("id".to_string(), json!(item_id));
            }
        }
        true
    } else if let Some(flat) = obj.get("name").and_then(Value::as_str) {
        if let Some(entry) = map.get(flat) {
            obj.insert("name".to_string(), json!(entry.name));
            obj.insert("namespace".to_string(), json!(entry.namespace));
            true
        } else {
            false
        }
    } else {
        false
    }
}

/// Wrap a native Responses SSE byte stream, restoring flattened namespace
/// calls and synthetic tool-search calls. Repair late arguments before changing
/// function-call types or item IDs. Unaffected events preserve their inner content.
pub(crate) fn create_tool_call_restore_sse_stream<E>(
    stream: impl Stream<Item = Result<Bytes, E>> + Send + 'static,
    map: HashMap<String, NamespacedName>,
    restore_tool_search: bool,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send
where
    E: std::error::Error + Send + 'static,
{
    let stream = super::responses_late_arguments::create_late_arguments_repair_stream(stream);
    async_stream::stream! {
        let mut buffer = String::new();
        let mut utf8_remainder: Vec<u8> = Vec::new();
        let mut tool_search_item_ids = HashMap::new();

        tokio::pin!(stream);

        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    append_utf8_safe(&mut buffer, &mut utf8_remainder, &bytes);
                    while let Some(block) = take_sse_block(&mut buffer) {
                        if block.trim().is_empty() {
                            continue;
                        }
                        yield Ok(restore_sse_block(
                            &block,
                            &map,
                            restore_tool_search,
                            &mut tool_search_item_ids,
                        ));
                    }
                }
                Err(e) => {
                    yield Err(std::io::Error::other(e.to_string()));
                    return;
                }
            }
        }

        // Flush any trailing partial block (streams normally end on a delimiter,
        // but be defensive so no bytes are dropped).
        if !utf8_remainder.is_empty() {
            buffer.push_str(&String::from_utf8_lossy(&utf8_remainder));
        }
        let tail = std::mem::take(&mut buffer);
        if !tail.trim().is_empty() {
            yield Ok(restore_sse_block(
                &tail,
                &map,
                restore_tool_search,
                &mut tool_search_item_ids,
            ));
        }
    }
}

/// Restore one SSE block. When the block's `data:` JSON carries an affected
/// function call, re-serialize just that line; otherwise the original block text
/// is preserved and only the `\n\n` delimiter re-appended.
fn restore_sse_block(
    block: &str,
    map: &HashMap<String, NamespacedName>,
    restore_tool_search: bool,
    tool_search_item_ids: &mut HashMap<String, String>,
) -> Bytes {
    let mut data_parts: Vec<&str> = Vec::new();
    for line in block.lines() {
        if let Some(data) = strip_sse_field(line, "data") {
            data_parts.push(data);
        }
    }

    if data_parts.is_empty() {
        return Bytes::from(format!("{block}\n\n"));
    }

    let data = data_parts.join("\n");
    if data.trim() == "[DONE]" {
        return Bytes::from(format!("{block}\n\n"));
    }

    let mut event: Value = match serde_json::from_str(&data) {
        Ok(value) => value,
        // Non-JSON data (shouldn't happen on the Responses wire): pass through.
        Err(_) => return Bytes::from(format!("{block}\n\n")),
    };

    if !restore_sse_event_tool_calls(&mut event, map, restore_tool_search, tool_search_item_ids) {
        return Bytes::from(format!("{block}\n\n"));
    }

    let restored = serde_json::to_string(&event).unwrap_or(data);
    rewrite_sse_json_data(block, &restored)
}

/// Replace only SSE data fields, preserving IDs, retry settings, event names,
/// comments and unknown fields. Multi-line data becomes one equivalent JSON line.
pub(crate) fn rewrite_sse_json_data(block: &str, data: &str) -> Bytes {
    let mut out = String::new();
    let mut wrote_data = false;
    for line in block.lines() {
        if strip_sse_field(line, "data").is_some() {
            if !wrote_data {
                out.push_str("data: ");
                out.push_str(data);
                out.push('\n');
                wrote_data = true;
            }
        } else {
            out.push_str(line);
            out.push('\n');
        }
    }
    out.push('\n');
    Bytes::from(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use serde_json::json;

    #[test]
    fn namespace_only_legacy_path_does_not_promote_or_rewrite_search_history() {
        let history = json!([
            {"type":"tool_search_call","call_id":"search","execution":"client","arguments":{"query":"write docs"}},
            {"type":"tool_search_output","call_id":"search","execution":"client","status":"completed","tools":[{"type":"namespace","name":"docs","tools":[{"type":"function","name":"write","input_schema":{"type":"object"}}]}]}
        ]);
        let mut body = json!({"tools":[{"type":"namespace","name":"docs","tools":[{"type":"function","name":"read","parameters":{"type":"object"}}]}],"input":history});
        let restore_map = namespace_restore_map(&body);
        assert!(restore_map.contains_key("docs__read"));
        assert!(!restore_map.contains_key("docs__write"));
        assert!(flatten_request_namespaces_only(&mut body).unwrap());
        assert_eq!(body["input"], history);
        assert_eq!(body["tools"].as_array().unwrap().len(), 1);
        assert_eq!(body["tools"][0]["name"], "docs__read");
        let mut search_only = json!({"input":history});
        let before = search_only.clone();
        assert!(!flatten_request_namespaces_only(&mut search_only).unwrap());
        assert_eq!(search_only, before);
    }

    #[test]
    fn native_sse_rewrites_only_data_and_preserves_resume_fields() {
        let block = concat!(
            "id: event-42\nretry: 1500\n: upstream heartbeat\n",
            "event: response.output_item.done\n",
            "data: {\"type\":\"response.output_item.done\",\n",
            "data: \"item\":{\"type\":\"function_call\",\"id\":\"fc_search\",\"call_id\":\"search\",\"name\":\"tool_search\",\"arguments\":\"{}\"}}\n",
            "x-gateway: kept"
        );
        let out = restore_sse_block(block, &HashMap::new(), true, &mut HashMap::new());
        let text = String::from_utf8(out.to_vec()).unwrap();
        assert!(text.starts_with(
            "id: event-42\nretry: 1500\n: upstream heartbeat\nevent: response.output_item.done\n"
        ));
        assert!(text.ends_with("x-gateway: kept\n\n"));
        assert_eq!(
            text.lines()
                .filter(|line| line.starts_with("data:"))
                .count(),
            1
        );
        let event: Value = serde_json::from_str(
            text.lines()
                .find_map(|line| line.strip_prefix("data: "))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(event["item"]["id"], "tsc_search");
        assert_eq!(event["item"]["type"], "tool_search_call");
    }

    #[test]
    fn explicit_server_search_is_rejected_without_execution_mode_changes() {
        for mut body in [
            json!({"tools":[{"type":"tool_search","execution":"server"}]}),
            json!({"input":[{"type":"tool_search_output","execution":"server","call_id":null,"status":"completed","tools":[]}]}),
        ] {
            let before = body.clone();
            assert!(prepare_native_responses_request(&mut body, true)
                .unwrap_err()
                .to_string()
                .contains("server-executed"));
            assert_eq!(body, before);
            assert!(
                super::super::transform_codex_chat::responses_to_chat_completions(body.clone())
                    .is_err()
            );
            assert!(
                super::super::transform_codex_anthropic::responses_request_to_anthropic(body, 4096)
                    .is_err()
            );
        }
    }

    #[test]
    fn native_prep_captures_real_provenance_and_preserves_schema() {
        let mut body =
            json!({"tools": [{"type":"function","name":"shell","parameters":{"type":"object"}}]});
        let context = prepare_native_responses_request(&mut body, true).unwrap();
        assert!(context.restore_tool_search);
        assert_eq!(body["tools"][1]["type"], "function");
        assert_eq!(body["tools"][1]["name"], "tool_search");
        assert_eq!(body["tools"][1]["parameters"]["required"], json!(["query"]));
        let once = body.clone();
        let repeated = prepare_native_responses_request(&mut body, true).unwrap();
        assert_eq!(body, once);
        assert!(
            !repeated.restore_tool_search,
            "a public function schema cannot manufacture provenance"
        );
        let mut response = json!({"output":[{"type":"function_call","name":"tool_search","call_id":"search","arguments":"{\"query\":\"docs\",\"limit\":\"4\"}"}]});
        assert!(restore_response_tool_calls(
            &mut response,
            &context.namespace_restore_map,
            context.restore_tool_search
        ));
        assert_eq!(response["output"][0]["type"], "tool_search_call");
        assert_eq!(response["output"][0]["arguments"]["limit"], 4);
    }

    #[test]
    fn native_prep_preserves_restrictive_choices_and_owned_names() {
        for choice in [
            json!("none"),
            json!("required"),
            json!({"type":"function","name":"shell"}),
            json!({"type":"allowed_tools","mode":"auto","tools":[{"type":"function","name":"shell"}]}),
        ] {
            for tools in [
                json!([{"type":"function","name":"shell","parameters":{}}]),
                json!([{"type":"tool_search"},{"type":"function","name":"shell","parameters":{}}]),
            ] {
                let mut body = json!({"tool_choice":choice,"tools":tools});
                let before = body.clone();
                let context = prepare_native_responses_request(&mut body, true).unwrap();
                assert_eq!(body, before);
                assert!(!context.restore_tool_search);
            }
        }
        for owned in [
            json!({"type":"function","name":"tool_search","parameters":{}}),
            json!({"type":"custom","name":"tool_search"}),
            json!("tool_search"),
            super::super::transform_codex_chat::tool_search_responses_function_tool(),
        ] {
            for native_first in [false, true] {
                let tools = if native_first {
                    json!([{"type":"tool_search"},owned])
                } else {
                    json!([owned])
                };
                let mut body = json!({"tools":tools});
                let before = body.clone();
                let context = prepare_native_responses_request(&mut body, true).unwrap();
                assert_eq!(body, before);
                assert!(!context.restore_tool_search);
            }
        }
    }

    #[test]
    fn native_prep_exact_search_selection_remains_forced() {
        let mut body = json!({"tool_choice":{"type":"tool_search"},"tools":[{"type":"tool_search","execution":"client","description":"Find a capability","parameters":{"type":"object","properties":{"goal":{"type":"string"}},"required":["goal"]}}]});
        let context = prepare_native_responses_request(&mut body, false).unwrap();
        assert!(context.restore_tool_search);
        assert_eq!(
            body["tool_choice"],
            json!({"type":"function","name":"tool_search"})
        );
        assert_eq!(body["tools"][0]["parameters"]["required"], json!(["goal"]));
        assert_eq!(body["tools"][0]["description"], "Find a capability");
    }

    #[test]
    fn native_prep_loaded_subset_activates_only_matching_inventory_identity() {
        let schema = json!({"type":"object","properties":{"original":{"type":"string"}}});
        let mut body = json!({"tools":[{"type":"namespace","name":"desktop","tools":[
            {"type":"function","name":"read","defer_loading":true,"inputSchema":schema},
            {"type":"function","name":"write","defer_loading":true,"parameters":{}}
        ]}],"input":[{"type":"tool_search_output","call_id":"search","execution":"client","status":"completed","tools":[{"type":"namespace","name":"desktop","tools":[{"type":"function","name":"read","deferLoading":true,"input_schema":{"type":"object","properties":{"wrong":{"type":"boolean"}}}}]}]}]});
        let context = prepare_native_responses_request(&mut body, true).unwrap();
        let tools = body["tools"].as_array().unwrap();
        let read = tools
            .iter()
            .find(|tool| tool["name"] == "desktop__read")
            .unwrap();
        assert_eq!(read["parameters"], schema);
        assert!(read.get("defer_loading").is_none());
        assert!(tools.iter().all(|tool| tool["name"] != "desktop__write"));
        assert!(context.namespace_restore_map.contains_key("desktop__read"));
        assert!(!context.namespace_restore_map.contains_key("desktop__write"));
        assert!(context.restore_tool_search);
    }

    #[test]
    fn loaded_child_of_deferred_namespace_keeps_siblings_deferred() {
        let body = json!({"tools":[{"type":"namespace","name":"docs","defer_loading":true,"tools":[{"type":"function","name":"read","parameters":{"type":"object"}},{"type":"function","name":"write","parameters":{"type":"object"}}]}],"input":[{"type":"tool_search_output","call_id":"search","status":"completed","execution":"client","tools":[{"type":"namespace","name":"docs","tools":[{"type":"function","name":"read","input_schema":{"type":"object"}}]}]}]});
        let chat = super::super::transform_codex_chat::build_codex_tool_context_from_request(&body);
        assert!(chat.lookup_chat_name("docs__read").is_some());
        assert!(chat.lookup_chat_name("docs__write").is_none());
        let mut native = body;
        let context = prepare_native_responses_request(&mut native, true).unwrap();
        assert!(context.namespace_restore_map.contains_key("docs__read"));
        assert!(!context.namespace_restore_map.contains_key("docs__write"));
        assert!(native["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "docs__read"));
        assert!(!native["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "docs__write"));
    }

    #[test]
    fn native_allowed_tool_names_are_rewritten_without_broadening() {
        let mut body = namespace_request();
        body["tool_choice"] = json!({"type":"allowed_tools","mode":"required","tools":[{"type":"function","namespace":"mcp__files__","name":"read"}]});
        let context = prepare_native_responses_request(&mut body, true).unwrap();
        assert!(!context.restore_tool_search);
        assert_eq!(
            body["tool_choice"],
            json!({"type":"allowed_tools","mode":"required","tools":[{"type":"function","name":"mcp__files____read"}]})
        );
    }

    #[test]
    fn native_prep_incomplete_or_nested_search_results_cannot_load_tools() {
        for marker in [json!({"status":"in_progress"}), json!({"status":"failed"})] {
            let mut output = json!({"type":"tool_search_output","call_id":"search","tools":[{"type":"function","name":"danger","defer_loading":true,"input_schema":{}}]});
            output
                .as_object_mut()
                .unwrap()
                .extend(marker.as_object().unwrap().clone());
            let mut body = json!({"input":[output]});
            let context = prepare_native_responses_request(&mut body, true).unwrap();
            assert!(!context.restore_tool_search);
            assert!(body.get("tools").is_none());
        }
        let mut body = json!({"input":[{"type":"function_call_output","call_id":"ordinary","output":{"type":"tool_search_output","call_id":"search","tools":[{"type":"function","name":"danger","inputSchema":{}}]}}]});
        let before = body.clone();
        assert!(
            !prepare_native_responses_request(&mut body, true)
                .unwrap()
                .restore_tool_search
        );
        assert_eq!(body, before);
    }

    #[test]
    fn native_prep_additional_tools_and_discovery_share_namespace_identity() {
        let mut body = json!({"input":[{"type":"additional_tools","role":"developer","tools":[{"type":"namespace","name":"docs","tools":[{"type":"function","name":"read","input_schema":{"type":"object"}}]}]},{"type":"tool_search_output","call_id":"search","tools":[{"type":"namespace","name":"docs","tools":[{"type":"function","name":"search","inputSchema":{"type":"object"},"defer_loading":true}]}]}]});
        let context = prepare_native_responses_request(&mut body, true).unwrap();
        assert_eq!(body["input"].as_array().unwrap().len(), 1);
        assert_eq!(context.namespace_restore_map.len(), 2);
        assert!(body["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "docs__read"));
        assert!(body["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "docs__search"));
    }

    #[test]
    fn native_additional_carriers_merge_children_of_existing_namespaces() {
        let function =
            |name: &str| json!({"type":"function","name":name,"input_schema":{"type":"object"}});
        let mut body = json!({"tools":[{"type":"namespace","name":"docs","tools":[function("read")]}],"input":[
            {"type":"additional_tools","tools":[{"type":"namespace","name":"docs","tools":[function("write")]}]},
            {"type":"additional_tools","tools":[{"type":"namespace","name":"docs","tools":[function("search"),function("read")]}]}
        ]});
        let context = prepare_native_responses_request(&mut body, true).unwrap();
        assert_eq!(context.namespace_restore_map.len(), 3);
        assert_eq!(body["tools"].as_array().unwrap().len(), 4);
        assert_eq!(body["input"], json!([]));
        for name in ["docs__read", "docs__write", "docs__search"] {
            assert_eq!(
                body["tools"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|tool| tool["name"] == name)
                    .count(),
                1
            );
        }
    }

    #[test]
    fn schema_alias_normalization_preserves_canonical_schema_and_true_loading_state() {
        for alias in ["inputSchema", "input_schema"] {
            let mut tool = json!({"type":"function","name":"read","defer_loading":true});
            tool[alias] = json!({"type":"object","properties":{"path":{"type":"string"}}});
            normalize_response_function_tool(&mut tool);
            assert_eq!(tool["parameters"]["properties"]["path"]["type"], "string");
            assert_eq!(tool["defer_loading"], true);
            assert!(tool.get(alias).is_none());
        }
        let mut tool = json!({"type":"function","name":"read","parameters":{"type":"object"},"inputSchema":{"type":"string"},"input_schema":{"type":"number"}});
        normalize_response_function_tool(&mut tool);
        assert_eq!(tool["parameters"], json!({"type":"object"}));
        assert!(tool.get("inputSchema").is_none() && tool.get("input_schema").is_none());
    }

    #[test]
    fn native_prep_rejects_namespace_wide_forcing_atomically() {
        let mut body = namespace_request();
        body["tool_choice"] = json!({"type":"namespace","name":"mcp__files__"});
        let before = body.clone();
        assert!(prepare_native_responses_request(&mut body, true).is_err());
        assert_eq!(body, before);
    }

    fn namespace_request() -> Value {
        json!({
            "model": "grok-4.5",
            "tools": [
                { "type": "function", "name": "plain_tool", "parameters": {} },
                {
                    "type": "namespace",
                    "name": "mcp__files__",
                    "tools": [
                        { "type": "function", "name": "read", "description": "read a file", "parameters": {} },
                        { "type": "function", "name": "write", "parameters": {} }
                    ]
                }
            ],
            "input": [
                {
                    "type": "function_call",
                    "name": "read",
                    "namespace": "mcp__files__",
                    "call_id": "c1",
                    "arguments": "{}"
                }
            ],
            "tool_choice": { "type": "function", "namespace": "mcp__files__", "name": "read" }
        })
    }

    #[test]
    fn flatten_lifts_namespace_children_to_top_level_functions() {
        let mut body = namespace_request();
        assert!(flatten_request_namespaces(&mut body).unwrap());

        let tools = body["tools"].as_array().unwrap();
        // plain + read + write, all top-level function tools; no namespace left.
        assert_eq!(tools.len(), 3);
        assert!(tools.iter().all(|t| t["type"] == "function"));
        let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
        assert!(names.contains(&"plain_tool"));
        assert!(names.contains(&"mcp__files____read"));
        assert!(names.contains(&"mcp__files____write"));
        // Child metadata is preserved on the lifted tool.
        let read = tools
            .iter()
            .find(|t| t["name"] == "mcp__files____read")
            .unwrap();
        assert_eq!(read["description"], "read a file");
    }

    #[test]
    fn flatten_rewrites_input_history_calls_and_tool_choice() {
        let mut body = namespace_request();
        flatten_request_namespaces(&mut body).unwrap();

        let call = &body["input"][0];
        assert_eq!(call["name"], "mcp__files____read");
        assert!(call.get("namespace").is_none());
        assert_eq!(call["call_id"], "c1");
        assert_eq!(
            body["tool_choice"],
            json!({"type": "function", "name": "mcp__files____read"})
        );
    }

    #[test]
    fn flatten_is_noop_without_namespace_tools() {
        let mut body = json!({
            "tools": [ { "type": "function", "name": "plain", "parameters": {} } ]
        });
        assert!(!flatten_request_namespaces(&mut body).unwrap());
    }

    #[test]
    fn native_followup_rewrites_tool_search_output_as_function_output() {
        let mut body = json!({
            "model": "gpt-5.6-sol",
            "tools": [{
                "type": "function",
                "name": "tool_search",
                "parameters": {"type": "object"}
            }],
            "input": [
                {
                    "type": "tool_search_call",
                    "call_id": "search-1",
                    "execution": "client",
                    "status": "completed",
                    "arguments": {"query": "automation_update", "limit": 8}
                },
                {
                    "type": "tool_search_output",
                    "call_id": "search-1",
                    "execution": "client",
                    "status": "completed",
                    "tools": []
                }
            ]
        });

        assert!(flatten_request_namespaces(&mut body).unwrap());
        let call = &body["input"][0];
        assert_eq!(call["type"], "function_call");
        assert_eq!(call["name"], "tool_search");
        assert_eq!(call["call_id"], "search-1");
        let arguments: Value = serde_json::from_str(call["arguments"].as_str().unwrap()).unwrap();
        assert_eq!(arguments["query"], "automation_update");
        assert_eq!(arguments["limit"], 8);

        let output = &body["input"][1];
        assert_eq!(output["type"], "function_call_output");
        assert_eq!(output["call_id"], "search-1");
        let payload: Value = serde_json::from_str(output["output"].as_str().unwrap()).unwrap();
        assert_eq!(payload["type"], "tool_search_output");
        assert_eq!(payload["tools"], json!([]));
    }

    #[test]
    fn native_compaction_disables_calls_and_preserves_replayed_tool_definitions() {
        let mut body = json!({
            "model": "grok-4.1-fast",
            "tool_choice": "auto",
            "parallel_tool_calls": true,
            "input": [{
                "type": "tool_search_call",
                "call_id": "search-1",
                "arguments": { "query": "docs" }
            }, {
                "type": "tool_search_output",
                "call_id": "search-1",
                "tools": [{
                    "type": "namespace",
                    "name": "docs",
                    "tools": [{
                        "type": "function",
                        "name": "search",
                        "parameters": { "type": "object" }
                    }]
                }]
            }, {
                "type": "compaction_trigger"
            }]
        });
        super::super::transform_codex_chat::inject_missing_tool_search(&mut body);
        assert!(flatten_request_namespaces(&mut body).unwrap());
        assert!(body["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "docs__search"));
        super::super::transform_codex_responses_xai_sanitize::sanitize_xai_responses_request(
            &mut body,
        );
        assert!(super::super::codex_compaction::prepare_native_third_party_request(&mut body));
        // v4.0.8 #7976 retains definitions for replay validation, with none
        // disabling new calls. Keep that official behavior rather than the old deletion.
        assert!(body["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "docs__search"));
        assert_eq!(body["tool_choice"], "none");
        assert_eq!(body["input"][0]["type"], "function_call");
        assert_eq!(body["input"][1]["type"], "function_call_output");
        assert!(serde_json::to_string(&body["input"][2])
            .unwrap()
            .contains("CONTEXT CHECKPOINT COMPACTION"));
    }

    #[test]
    fn native_followup_promotes_discovered_namespace_tools_and_restores_identity() {
        let mut body = json!({
            "model": "gpt-5.6-sol",
            "tools": [{
                "type": "function",
                "name": "tool_search",
                "parameters": {"type": "object"}
            }],
            "input": [{
                "type": "tool_search_output",
                "call_id": "search-1",
                "execution": "client",
                "status": "completed",
                "tools": [{
                    "type": "namespace",
                    "name": "codex_app",
                    "tools": [{
                        "type": "function",
                        "name": "automation_update",
                        "description": "Manage an automation",
                        "inputSchema": {"type": "object"},
                        "deferLoading": false
                    }]
                }]
            }]
        });

        let mut prepared = body.clone();
        let restore_map = prepare_native_responses_request(&mut prepared, false)
            .unwrap()
            .namespace_restore_map;
        let entry = restore_map.get("codex_app__automation_update").unwrap();
        assert_eq!(entry.namespace, "codex_app");
        assert_eq!(entry.name, "automation_update");

        assert!(flatten_request_namespaces(&mut body).unwrap());
        let names: Vec<&str> = body["tools"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str))
            .collect();
        assert!(names.contains(&"tool_search"));
        assert!(names.contains(&"codex_app__automation_update"));
        let automation = body["tools"]
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == "codex_app__automation_update")
            .unwrap();
        assert_eq!(automation["parameters"], json!({"type": "object"}));
        assert!(automation.get("inputSchema").is_none());
        assert!(automation.get("deferLoading").is_none());
        assert_eq!(body["input"][0]["type"], "function_call_output");

        let mut response = json!({
            "output": [{
                "type": "function_call",
                "name": "codex_app__automation_update",
                "call_id": "automation-1",
                "arguments": "{}"
            }]
        });
        assert!(restore_response_tool_calls(
            &mut response,
            &restore_map,
            true
        ));
        assert_eq!(response["output"][0]["name"], "automation_update");
        assert_eq!(response["output"][0]["namespace"], "codex_app");
    }

    #[test]
    fn native_followup_merges_discovered_children_into_existing_namespace() {
        let mut body = json!({
            "model": "gpt-5.6-sol",
            "tools": [
                {
                    "type": "function",
                    "name": "tool_search",
                    "parameters": {"type": "object"}
                },
                {
                    "type": "namespace",
                    "name": "codex_app",
                    "description": "Tools provided by the Codex app.",
                    "tools": [{
                        "type": "function",
                        "name": "read_thread",
                        "parameters": {"type": "object"}
                    }]
                }
            ],
            "input": [{
                "type": "tool_search_output",
                "call_id": "search-1",
                "execution": "client",
                "status": "completed",
                "tools": [{
                    "type": "namespace",
                    "name": "codex_app",
                    "description": "Tools provided by the Codex app.",
                    "tools": [{
                        "type": "function",
                        "name": "list_projects",
                        "parameters": {
                            "type": "object",
                            "properties": {},
                            "additionalProperties": false
                        },
                        "defer_loading": false
                    }]
                }]
            }]
        });

        assert!(flatten_request_namespaces(&mut body).unwrap());
        let names: Vec<&str> = body["tools"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str))
            .collect();
        assert!(names.contains(&"codex_app__read_thread"));
        assert!(
            names.contains(&"codex_app__list_projects"),
            "discovered children from an existing namespace must be merged: {names:?}"
        );
    }

    #[test]
    fn native_followup_preserves_none_and_does_not_activate_discovered_tools() {
        let mut body = json!({
            "tool_choice": "none",
            "input": [{"type": "tool_search_output", "call_id": "search-1", "tools": [{
                "type": "function", "name": "create_thread", "defer_loading": true,
                "input_schema": {"type": "object"}
            }]}]
        });
        let context = prepare_native_responses_request(&mut body, true).unwrap();
        assert_eq!(body["tool_choice"], "none");
        assert!(body.get("tools").is_none());
        assert!(!context.restore_tool_search);
        assert_eq!(body["input"][0]["type"], "function_call_output");
    }

    #[test]
    fn flatten_rewrites_only_direct_protocol_input_items() {
        let nested_call = json!({
            "type": "function_call",
            "namespace": "mcp__files__",
            "name": "read",
            "call_id": "nested-call",
            "arguments": "{}"
        });
        let mut body = json!({
            "tools": [{
                "type": "namespace",
                "name": "mcp__files__",
                "tools": [{"type": "function", "name": "read", "parameters": {}}]
            }],
            "input": [
                {
                    "type": "function_call",
                    "namespace": "mcp__files__",
                    "name": "read",
                    "call_id": "direct-call",
                    "arguments": "{}"
                },
                {
                    "type": "function_call_output",
                    "call_id": "outer-call",
                    "output": {"result": nested_call.clone()}
                }
            ]
        });

        assert!(flatten_request_namespaces(&mut body).unwrap());
        assert_eq!(body["input"][0]["name"], "mcp__files____read");
        assert!(body["input"][0].get("namespace").is_none());
        assert_eq!(body["input"][1]["output"]["result"], nested_call);
    }

    #[test]
    fn nested_tool_search_output_in_function_payload_is_not_rewritten() {
        let nested = json!({
            "type": "tool_search_output",
            "tools": [{"type": "function", "name": "loaded", "parameters": {}}]
        });
        let mut body = json!({
            "tools": [{"type": "function", "name": "shell", "parameters": {}}],
            "input": [{"type": "function_call_output", "call_id": "call-1", "output": nested.clone()}]
        });

        assert!(!flatten_request_namespaces(&mut body).unwrap());
        assert_eq!(body["input"][0]["output"], nested);
        assert!(body["tools"]
            .as_array()
            .unwrap()
            .iter()
            .all(|tool| tool["name"] != "loaded"));
    }

    #[test]
    fn flatten_errors_on_flat_name_collision_with_top_level() {
        let mut body = json!({
            "tools": [
                { "type": "function", "name": "mcp__files____read", "parameters": {} },
                {
                    "type": "namespace",
                    "name": "mcp__files__",
                    "tools": [ { "type": "function", "name": "read", "parameters": {} } ]
                }
            ]
        });
        assert!(flatten_request_namespaces(&mut body).is_err());
    }

    #[test]
    fn restore_map_inverts_flatten_naming() {
        let body = namespace_request();
        let map = namespace_restore_map(&body);
        let entry = map.get("mcp__files____read").unwrap();
        assert_eq!(entry.namespace, "mcp__files__");
        assert_eq!(entry.name, "read");
        // Plain top-level tools are not in the restore map.
        assert!(!map.contains_key("plain_tool"));
    }

    #[test]
    fn round_trip_flatten_then_restore_recovers_namespace() {
        let request = namespace_request();
        let map = namespace_restore_map(&request);

        // Upstream returns a function_call using the flattened name.
        let mut response = json!({
            "type": "response",
            "output": [
                {
                    "type": "function_call",
                    "name": "mcp__files____read",
                    "call_id": "c1",
                    "arguments": "{}"
                }
            ]
        });
        assert!(restore_response_tool_calls(&mut response, &map, false));
        let call = &response["output"][0];
        assert_eq!(call["name"], "read");
        assert_eq!(call["namespace"], "mcp__files__");
    }

    #[test]
    fn restore_leaves_unmapped_calls_untouched() {
        let map = namespace_restore_map(&namespace_request());
        let mut response = json!({
            "output": [
                { "type": "function_call", "name": "plain_tool", "call_id": "x" }
            ]
        });
        assert!(!restore_response_tool_calls(&mut response, &map, false));
        assert_eq!(response["output"][0]["name"], "plain_tool");
        assert!(response["output"][0].get("namespace").is_none());
    }

    #[test]
    fn restore_converts_tool_search_function_call_for_native_responses() {
        let mut response = json!({
            "output": [
                {
                    "id": "fc_search-1",
                    "type": "function_call",
                    "name": "tool_search",
                    "call_id": "search-1",
                    "status": "completed",
                    "arguments": "{\"query\":\"automation\",\"limit\":\"8\"}"
                },
                {
                    "id": "fc_regular-1",
                    "type": "function_call",
                    "name": "plain_tool",
                    "call_id": "regular-1",
                    "status": "completed",
                    "arguments": "{}"
                }
            ]
        });

        assert!(restore_response_tool_calls(
            &mut response,
            &HashMap::new(),
            true
        ));
        let call = &response["output"][0];
        assert_eq!(call["id"], "tsc_search-1");
        assert_eq!(call["type"], "tool_search_call");
        assert_eq!(call["execution"], "client");
        assert_eq!(call["call_id"], "search-1");
        assert_eq!(call["status"], "completed");
        assert_eq!(call["arguments"]["query"], "automation");
        assert_eq!(call["arguments"]["limit"], 8);
        assert!(call.get("name").is_none());
        assert_eq!(response["output"][1]["id"], "fc_regular-1");
        assert_eq!(response["output"][1]["type"], "function_call");
    }

    #[test]
    fn restore_ignores_function_call_shaped_metadata() {
        let metadata_call = json!({
            "id": "fc_metadata",
            "type": "function_call",
            "name": "tool_search",
            "call_id": "metadata-call",
            "arguments": "{}"
        });
        let mut response = json!({
            "metadata": {"echo": metadata_call.clone()},
            "output": [{
                "id": "fc_output",
                "type": "function_call",
                "name": "tool_search",
                "call_id": "output-call",
                "arguments": "{}"
            }]
        });

        assert!(restore_response_tool_calls(
            &mut response,
            &HashMap::new(),
            true
        ));
        assert_eq!(response["metadata"]["echo"], metadata_call);
        assert_eq!(response["output"][0]["type"], "tool_search_call");

        let mut event = json!({
            "type": "response.output_item.added",
            "metadata": {"echo": metadata_call.clone()},
            "item": {
                "id": "fc_event",
                "type": "function_call",
                "name": "tool_search",
                "call_id": "event-call",
                "arguments": "{}"
            }
        });
        let mut item_ids = HashMap::new();

        assert!(restore_sse_event_tool_calls(
            &mut event,
            &HashMap::new(),
            true,
            &mut item_ids
        ));
        assert_eq!(event["metadata"]["echo"], metadata_call);
        assert_eq!(event["item"]["type"], "tool_search_call");
    }

    #[test]
    fn long_flat_names_stay_consistent_between_flatten_and_restore() {
        let long_child = "a".repeat(80);
        let body = json!({
            "tools": [{
                "type": "namespace",
                "name": "mcp__srv__",
                "tools": [ { "type": "function", "name": long_child, "parameters": {} } ]
            }]
        });
        let mut flattened = body.clone();
        flatten_request_namespaces(&mut flattened).unwrap();
        let flat_name = flattened["tools"][0]["name"].as_str().unwrap().to_string();
        // Truncation kicks in past the 64-char chat tool-name limit.
        assert!(flat_name.len() <= 64);

        let map = namespace_restore_map(&body);
        // The truncated name from flatten must be a restore-map key.
        let entry = map.get(&flat_name).unwrap();
        assert_eq!(entry.namespace, "mcp__srv__");
        assert_eq!(entry.name, long_child);
    }

    #[tokio::test]
    async fn sse_stream_restores_function_call_events_and_passes_others_through() {
        let map = namespace_restore_map(&namespace_request());

        let added = "event: response.output_item.added\n\
                     data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"function_call\",\"name\":\"mcp__files____read\",\"call_id\":\"c1\"}}\n\n";
        let delta = "event: response.output_text.delta\n\
                     data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n";
        let done = "data: [DONE]\n\n";

        let chunks = vec![
            Ok::<Bytes, std::io::Error>(Bytes::from(added)),
            Ok(Bytes::from(delta)),
            Ok(Bytes::from(done)),
        ];
        let input = stream::iter(chunks);
        let out = create_tool_call_restore_sse_stream(input, map, false);
        futures::pin_mut!(out);

        let mut collected = String::new();
        while let Some(chunk) = out.next().await {
            collected.push_str(std::str::from_utf8(&chunk.unwrap()).unwrap());
        }

        // function_call name restored to namespace form.
        assert!(collected.contains("\"name\":\"read\""));
        assert!(collected.contains("\"namespace\":\"mcp__files__\""));
        assert!(!collected.contains("mcp__files____read"));
        // Unrelated events preserved verbatim.
        assert!(collected.contains("\"delta\":\"hi\""));
        assert!(collected.contains("[DONE]"));
    }

    #[tokio::test]
    async fn sse_stream_repairs_late_arguments_before_tool_search_restore() {
        let item = |arguments: &str| {
            json!({
                "id": "fc_search-1", "type": "function_call", "name": "tool_search",
                "call_id": "search-1", "status": "completed", "arguments": arguments
            })
        };
        let arguments = r#"{"query":"thread tools","limit":"2"}"#;
        let events = vec![
            json!({ "type": "response.output_item.added", "output_index": 0,
                    "item": item("") }),
            json!({ "type": "response.function_call_arguments.done", "output_index": 0,
                    "item_id": "fc_search-1", "arguments": "" }),
            json!({ "type": "response.output_item.done", "output_index": 0,
                    "item": item("") }),
            json!({ "type": "response.function_call_arguments.delta", "output_index": 0,
                    "item_id": "fc_search-1", "delta": r#"{"query":"thread tools","# }),
            json!({ "type": "response.function_call_arguments.delta", "output_index": 0,
                    "item_id": "fc_search-1", "delta": r#""limit":"2"}"# }),
            json!({ "type": "response.completed", "response": { "output": [item(arguments)] } }),
        ];
        let input =
            stream::iter(events.into_iter().map(|event| {
                Ok::<Bytes, std::io::Error>(Bytes::from(format!("data: {event}\n\n")))
            }));
        let out = create_tool_call_restore_sse_stream(input, HashMap::new(), true);
        futures::pin_mut!(out);
        let mut collected = String::new();
        while let Some(chunk) = out.next().await {
            collected.push_str(std::str::from_utf8(&chunk.unwrap()).unwrap());
        }
        let events: Vec<Value> = collected
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .map(|data| serde_json::from_str(data).unwrap())
            .collect();

        assert_eq!(events[1]["type"], "response.function_call_arguments.delta");
        assert_eq!(events[2]["type"], "response.function_call_arguments.delta");
        assert_eq!(events[3]["type"], "response.function_call_arguments.done");
        assert_eq!(events[3]["arguments"], arguments);
        for event in &events[1..=3] {
            assert_eq!(event["item_id"], "tsc_search-1");
        }
        let restored = &events[4]["item"];
        assert_eq!(restored["type"], "tool_search_call");
        assert_eq!(restored["id"], "tsc_search-1");
        assert_eq!(restored["execution"], "client");
        assert_eq!(
            restored["arguments"],
            json!({ "query": "thread tools", "limit": 2 })
        );
        assert_eq!(events[5]["response"]["output"][0], *restored);
    }

    #[tokio::test]
    async fn sse_stream_repairs_late_arguments_before_namespace_restore() {
        let map = namespace_restore_map(&namespace_request());
        let item = |arguments: &str| {
            json!({
                "id": "fc_read", "type": "function_call", "name": "mcp__files____read",
                "call_id": "read-1", "arguments": arguments
            })
        };
        let arguments = r#"{"path":"notes.txt"}"#;
        // Some gateways provide the arguments only in response.completed.
        let events = vec![
            json!({ "type": "response.output_item.added", "item": item("") }),
            json!({ "type": "response.function_call_arguments.done",
                    "item_id": "fc_read", "arguments": "" }),
            json!({ "type": "response.output_item.done", "item": item("") }),
            json!({ "type": "response.completed", "response": { "output": [item(arguments)] } }),
        ];
        let input =
            stream::iter(events.into_iter().map(|event| {
                Ok::<Bytes, std::io::Error>(Bytes::from(format!("data: {event}\n\n")))
            }));
        let out = create_tool_call_restore_sse_stream(input, map, false);
        futures::pin_mut!(out);
        let mut collected = String::new();
        while let Some(chunk) = out.next().await {
            collected.push_str(std::str::from_utf8(&chunk.unwrap()).unwrap());
        }
        let events: Vec<Value> = collected
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .map(|data| serde_json::from_str(data).unwrap())
            .collect();

        assert_eq!(events[1]["arguments"], arguments);
        let restored = &events[2]["item"];
        assert_eq!(restored["type"], "function_call");
        assert_eq!(restored["name"], "read");
        assert_eq!(restored["namespace"], "mcp__files__");
        assert_eq!(restored["arguments"], arguments);
        assert_eq!(events[3]["response"]["output"][0], *restored);
    }

    #[tokio::test]
    async fn sse_stream_restores_native_tool_search_function_call() {
        let added = "event: response.output_item.added\n\
                     data: {\"type\":\"response.output_item.added\",\"item\":{\"id\":\"fc_search-1\",\"type\":\"function_call\",\"name\":\"tool_search\",\"call_id\":\"search-1\",\"status\":\"in_progress\",\"arguments\":\"\"}}\n\n";
        let arguments_delta = "event: response.function_call_arguments.delta\n\
                               data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc_search-1\",\"output_index\":0,\"delta\":\"{\\\"query\\\":\\\"thread tools\\\"}\"}\n\n";
        let arguments_done = "event: response.function_call_arguments.done\n\
                              data: {\"type\":\"response.function_call_arguments.done\",\"item_id\":\"fc_search-1\",\"output_index\":0,\"arguments\":\"{\\\"query\\\":\\\"thread tools\\\",\\\"limit\\\":8}\"}\n\n";
        let done = "event: response.output_item.done\n\
                    data: {\"type\":\"response.output_item.done\",\"item\":{\"id\":\"fc_search-1\",\"type\":\"function_call\",\"name\":\"tool_search\",\"call_id\":\"search-1\",\"status\":\"completed\",\"arguments\":\"{\\\"query\\\":\\\"thread tools\\\",\\\"limit\\\":8}\"}}\n\n";
        let completed = "event: response.completed\n\
                         data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"id\":\"fc_search-1\",\"type\":\"function_call\",\"name\":\"tool_search\",\"call_id\":\"search-1\",\"status\":\"completed\",\"arguments\":\"{\\\"query\\\":\\\"thread tools\\\",\\\"limit\\\":8}\"}]}}\n\n";
        let input = stream::iter(vec![
            Ok::<Bytes, std::io::Error>(Bytes::from(added)),
            Ok(Bytes::from(arguments_delta)),
            Ok(Bytes::from(arguments_done)),
            Ok(Bytes::from(done)),
            Ok(Bytes::from(completed)),
            Ok(Bytes::from("data: [DONE]\n\n")),
        ]);
        let out = create_tool_call_restore_sse_stream(input, HashMap::new(), true);
        futures::pin_mut!(out);

        let mut collected = String::new();
        while let Some(chunk) = out.next().await {
            collected.push_str(std::str::from_utf8(&chunk.unwrap()).unwrap());
        }

        assert_eq!(
            collected.matches("\"type\":\"tool_search_call\"").count(),
            3
        );
        assert_eq!(collected.matches("\"id\":\"tsc_search-1\"").count(), 3);
        assert_eq!(collected.matches("\"item_id\":\"tsc_search-1\"").count(), 2);
        assert!(!collected.contains("fc_search-1"));
        assert!(collected.contains("\"execution\":\"client\""));
        assert!(collected.contains("\"query\":\"thread tools\""));
        assert!(collected.contains("\"limit\":8"));
        assert!(!collected.contains("\"name\":\"tool_search\""));
    }
}
