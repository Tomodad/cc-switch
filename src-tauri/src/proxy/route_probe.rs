//! Explicitly enabled, bounded metadata observation for one synthetic delegation.

use super::hyper_client::ProxyResponse;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

const MAX_EVENTS: usize = 64;
const MAX_FRAME: usize = 65_536;
// WS messages already reside in the forwarding layer; parsing shares the existing
// 1 MiB cumulative observation budget. HTTP framing/output storage remain 64 KiB.
const MAX_WS_BYTES: usize = 1_048_576;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    version: u8,
    enabled: bool,
    run_id: String,
    source_task: String,
    target_task: String,
    marker: String,
    provider_id: String,
    expires_ms: u64,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn digest(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

fn load_config(path: &PathBuf) -> Option<Config> {
    if fs::metadata(path).ok()?.len() > 4096 {
        return None;
    }
    serde_json::from_slice(&fs::read(path).ok()?).ok()
}

impl Config {
    fn valid(&self) -> bool {
        let now = now_ms();
        self.version == 1
            && self.enabled
            && self.expires_ms > now
            && self.expires_ms - now <= 120_000
            && [&self.run_id, &self.source_task, &self.target_task]
                .iter()
                .all(|s| uuid::Uuid::parse_str(s).is_ok())
            && self
                .marker
                .strip_prefix("CCSWITCH_ROUTE_PROBE_")
                .is_some_and(|n| n.len() == 32 && n.bytes().all(|b| b.is_ascii_hexdigit()))
            && !self.provider_id.is_empty()
            && self.provider_id.len() <= 128
    }
}

fn last_input_text(body: &Value) -> Option<&str> {
    let item = body.get("input")?.as_array()?.last()?;
    if item.get("type").and_then(Value::as_str) == Some("function_call_output") {
        return item.get("output")?.as_str();
    }
    if item.get("role").and_then(Value::as_str) == Some("user") {
        let content = item.get("content")?;
        return content.as_str().or_else(|| {
            let parts = content.as_array()?;
            if parts.len() != 1 {
                return None;
            }
            parts[0].get("text")?.as_str()
        });
    }
    None
}

fn matches_probe_input(body: &Value, marker: &str, source: &str, target: &str) -> bool {
    let Some(text) = last_input_text(body).filter(|s| s.len() <= 4096) else {
        return false;
    };
    // Match the newest delegation only, never an old marker elsewhere in history.
    let text = text.trim();
    text.starts_with("<codex_delegation>")
        && text.ends_with("</codex_delegation>")
        && text.contains(&format!("<source_thread_id>{source}</source_thread_id>"))
        && text.contains(&format!("Reply only {marker}."))
        && text.contains(&format!("Target task: {target}."))
}

fn route(url: &str) -> Option<Value> {
    let u = url::Url::parse(url).ok()?;
    if !matches!(u.scheme(), "http" | "https" | "ws" | "wss") {
        return None;
    }
    let path = u.path();
    let safe_path = matches!(
        path,
        "/responses"
            | "/v1/responses"
            | "/v1/v1/responses"
            | "/codex/v1/responses"
            | "/v1/chat/completions"
            | "/chat/completions"
            | "/v1/messages"
            | "/messages"
    );
    Some(
        json!({"scheme":u.scheme(), "host":u.host_str()?, "port":u.port_or_known_default(),
        "path":if safe_path { path.to_owned() } else { format!("sha256:{}", digest(path)) }}),
    )
}

struct Sink {
    config: Config,
    path: PathBuf,
    started: Instant,
    state: Mutex<(File, usize, usize, usize)>,
}

impl Sink {
    fn active(&self) -> bool {
        self.started.elapsed().as_secs() < 120
            && now_ms() < self.config.expires_ms
            && load_config(&self.path).is_some_and(|c| {
                c.enabled
                    && c.run_id == self.config.run_id
                    && c.marker == self.config.marker
                    && c.source_task == self.config.source_task
                    && c.target_task == self.config.target_task
                    && c.provider_id == self.config.provider_id
            })
    }

    fn write(&self, attempt: &str, phase: &str, detail: Value) {
        if !self.active() {
            return;
        }
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.1 >= MAX_EVENTS {
            return;
        }
        let record = json!({"version":1, "pid":std::process::id(), "run_id":self.config.run_id,
            "source_task":self.config.source_task,"target_task":self.config.target_task,
            "marker_sha256":digest(&self.config.marker),"attempt_id":attempt,
            "seq":state.1,"ts_ms":now_ms(),"phase":phase,"detail":detail});
        // Only locally constructed allowlisted metadata reaches disk; never raw input/errors.
        if let Ok(mut line) = serde_json::to_vec(&record) {
            line.push(b'\n');
            if line.len() > 4096 || state.2 + line.len() > MAX_FRAME {
                return;
            }
            if state.0.write_all(&line).is_ok() {
                state.1 += 1;
                state.2 += line.len();
            }
        }
    }
}

pub(crate) struct Attempt {
    sink: Arc<Sink>,
    id: String,
    received: Mutex<(usize, usize)>,
    requested_route: Value,
}

impl Attempt {
    pub(crate) fn note(&self, phase: &str) {
        // Callers pass fixed phase constants, never an upstream error or header.
        if matches!(
            phase,
            "downstream_upgrade" | "upstream_upgrade" | "request_sent" | "send_failed" | "closed"
        ) {
            self.sink.write(&self.id, phase, json!({}));
        }
    }

    pub(crate) fn response_event(&self, value: &Value) {
        let kind = value.get("type").and_then(Value::as_str).unwrap_or("");
        let body = value.get("response").unwrap_or(value);
        let status = body.get("status").and_then(Value::as_str);
        let terminal = match kind {
            "response.completed" => match status {
                Some("completed") => "completed",
                Some("failed") => "failed",
                Some("incomplete") => "incomplete",
                _ => return,
            },
            "response.failed" | "error" => "failed",
            "response.incomplete" => "incomplete",
            _ if status == Some("completed") => "completed",
            _ if status == Some("failed") => "failed",
            _ => return,
        };
        let mut text = String::new();
        let mut text_bytes = 0;
        let mut other_items = 0;
        let output = body.get("output").and_then(Value::as_array);
        if let Some(items) = output {
            for item in items {
                if item.get("type").and_then(Value::as_str) == Some("reasoning") {
                    continue;
                }
                if item.get("type").and_then(Value::as_str) != Some("message")
                    || item.get("role").and_then(Value::as_str) != Some("assistant")
                {
                    other_items += 1;
                    continue;
                }
                for part in item
                    .get("content")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    if part.get("type").and_then(Value::as_str) != Some("output_text") {
                        continue;
                    }
                    if let Some(s) = part.get("text").and_then(Value::as_str) {
                        text_bytes += s.len();
                        if text_bytes <= 256 {
                            text.push_str(s);
                        }
                    }
                }
            }
        }
        // IDs are hashed as even an unexpected ID string could contain a credential.
        let response_id = body
            .get("id")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty() && s.len() <= 256);
        let (frames, bytes) = self.received.lock().map(|s| *s).unwrap_or_default();
        self.sink.write(&self.id, "terminal", json!({
            "terminal":terminal, "response_id_sha256":response_id.map(digest),
            "output_present":output.is_some(), "text_bytes":text_bytes,
            "observed_frames":frames, "observed_bytes":bytes,
            "marker_exact":text_bytes <= 256 && other_items == 0 && text.trim() == self.sink.config.marker,
            "other_items":other_items
        }));
    }

    pub(crate) fn ws_text(&self, text: &str) {
        if !self.sink.active() {
            return;
        }
        if let Ok(mut s) = self.received.lock() {
            s.0 = s.0.saturating_add(1);
            s.1 = s.1.saturating_add(text.len());
            if s.0 > 512 || s.1 > MAX_WS_BYTES {
                self.sink.write(
                    &self.id,
                    "observation_limit",
                    json!({
                        "reason":if s.0 > 512 { "ws_frame_count" } else { "ws_total_bytes" },
                        "frame_bytes":text.len(), "observed_frames":s.0, "observed_bytes":s.1
                    }),
                );
                return;
            }
        }
        if text.len() > MAX_WS_BYTES {
            self.sink.write(
                &self.id,
                "observation_limit",
                json!({
                    "reason":"ws_frame_bytes", "frame_bytes":text.len()
                }),
            );
            return;
        }
        if let Ok(v) = serde_json::from_str(text) {
            self.response_event(&v);
        }
    }

    pub(crate) fn wrap_http(self, response: ProxyResponse) -> ProxyResponse {
        let status = response.status();
        let headers = response.headers().clone();
        let sse = response.is_sse();
        let encoded = headers
            .get("content-encoding")
            .is_some_and(|v| v != "identity");
        let final_route = match &response {
            ProxyResponse::Reqwest(r) => route(r.url().as_str()),
            _ => Some(self.requested_route.clone()),
        };
        // This confirms receipt of response headers, not the send-start timestamp.
        self.sink.write(
            &self.id,
            "response_headers",
            json!({
                "status":status.as_u16(), "sse":sse, "encoded":encoded,
                "redirected":final_route.as_ref() != Some(&self.requested_route),
                "final_route":final_route
            }),
        );
        let wrapped = async_stream::stream! {
            let stream = response.bytes_stream();
            tokio::pin!(stream);
            let mut buffer = Vec::new();
            let mut limited = encoded;
            while let Some(chunk) = stream.next().await {
                if !self.sink.active() {
                    limited = true;
                    buffer.clear();
                }
                if let Ok(bytes) = &chunk {
                    if !limited {
                        if buffer.len() + bytes.len() > MAX_FRAME {
                            limited = true;
                            buffer.clear();
                            self.sink.write(&self.id, "observation_limit", json!({}));
                        } else {
                            buffer.extend_from_slice(bytes);
                            if sse {
                                // Retain incomplete framing in memory only; forward original bytes unchanged.
                                while let Some((pos, width)) = {
                                    let lf = buffer.windows(2).position(|w| w == b"\n\n").map(|p| (p,2));
                                    let crlf = buffer.windows(4).position(|w| w == b"\r\n\r\n").map(|p| (p,4));
                                    lf.into_iter().chain(crlf).min_by_key(|p| p.0)
                                } {
                                    let end = pos + width;
                                    let block: Vec<u8> = buffer.drain(..end).collect();
                                    if let Ok(s) = std::str::from_utf8(&block) {
                                        let data = s.lines().filter_map(|l| l.strip_prefix("data:"))
                                            .map(str::trim_start).collect::<Vec<_>>().join("\n");
                                        self.ws_text(&data);
                                    }
                                }
                            }
                        }
                    }
                }
                yield chunk;
            }
            if !sse && !limited {
                if let Ok(s) = std::str::from_utf8(&buffer) { self.ws_text(s); }
            }
        };
        ProxyResponse::streamed(status, headers, wrapped)
    }
}

impl Drop for Attempt {
    fn drop(&mut self) {
        self.note("closed");
    }
}

/// No environment opt-in means no config read, output file or observation.
pub(crate) fn is_armed() -> bool {
    std::env::var_os("CC_SWITCH_ROUTE_PROBE_CONFIG")
        .map(PathBuf::from)
        .and_then(|p| load_config(&p))
        .is_some_and(|c| c.valid())
}

pub(crate) fn begin(
    body: &Value,
    provider: &crate::provider::Provider,
    url: &str,
    transport: &str,
    adapter: &str,
) -> Option<Attempt> {
    let path = PathBuf::from(std::env::var_os("CC_SWITCH_ROUTE_PROBE_CONFIG")?);
    static ACTIVE: OnceLock<Mutex<Option<Arc<Sink>>>> = OnceLock::new();
    begin_at(
        path,
        body,
        provider,
        (url, transport, adapter),
        ACTIVE.get_or_init(|| Mutex::new(None)),
    )
}

fn begin_at(
    path: PathBuf,
    body: &Value,
    provider: &crate::provider::Provider,
    upstream: (&str, &str, &str),
    registry: &Mutex<Option<Arc<Sink>>>,
) -> Option<Attempt> {
    if !path.is_absolute() || path.to_string_lossy().starts_with(r"\\") {
        return None;
    }
    let (url, transport, adapter) = upstream;
    let c = load_config(&path)?;
    if !c.valid()
        || c.provider_id != provider.id
        || super::providers::is_codex_official_provider(provider)
        || !matches_probe_input(body, &c.marker, &c.source_task, &c.target_task)
    {
        return None;
    }
    let route = route(url)?;
    let mut current = registry.lock().ok()?;
    let sink = if let Some(s) = current.as_ref() {
        if s.config.run_id != c.run_id || !s.active() {
            return None;
        }
        s.clone()
    } else {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path.parent()?.join("events.jsonl"))
            .ok()?;
        let s = Arc::new(Sink {
            config: c,
            path,
            started: Instant::now(),
            state: Mutex::new((file, 0, 0, 0)),
        });
        *current = Some(s.clone());
        s
    };
    {
        let mut state = sink.state.lock().ok()?;
        if state.3 >= 4 {
            return None;
        }
        state.3 += 1;
    }
    let attempt = Attempt {
        sink,
        id: uuid::Uuid::new_v4().to_string(),
        received: Mutex::new((0, 0)),
        requested_route: route.clone(),
    };
    let adapter = match adapter {
        "native_responses" | "chat" | "anthropic" => adapter,
        _ => "unknown",
    };
    attempt.sink.write(&attempt.id, "selected", json!({
        "provider_id_sha256":digest(&provider.id), "route":route,
        "upstream_transport":if transport == "ws" { "ws" } else { "http" },
        "adapter":adapter, "input_items":body.get("input").and_then(Value::as_array).map(Vec::len),
        "last_item_type":body.get("input").and_then(Value::as_array).and_then(|v| v.last())
            .map(|v| if v.get("type").and_then(Value::as_str) == Some("function_call_output") {
                "function_call_output"
            } else { "user_message" }),
        "call_id_present":body.get("input").and_then(Value::as_array).and_then(|v| v.last())
            .is_some_and(|v| v.get("call_id").and_then(Value::as_str).is_some()),
        "model_sha256":body.get("model").and_then(Value::as_str).map(digest),
        "generate":body.get("generate").and_then(Value::as_bool),
        "matched_item_sha256":last_input_text(body).map(digest)
    }));
    Some(attempt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const MARKER: &str = "CCSWITCH_ROUTE_PROBE_0123456789abcdef0123456789abcdef";
    const SOURCE: &str = "10000000-0000-4000-8000-000000000001";
    const TARGET: &str = "20000000-0000-4000-8000-000000000002";

    fn request() -> serde_json::Value {
        // Synthetic fixture, not a captured incident request.
        json!({"input": [{"type":"function_call_output", "output":format!(
            "<codex_delegation>\n<source_thread_id>{SOURCE}</source_thread_id>\n<input>Reply only {MARKER}. Target task: {TARGET}. Do not use tools.</input>\n</codex_delegation>"
        )}]})
    }

    #[test]
    fn route_probe_selects_only_exact_scoped_delegation() {
        assert!(matches_probe_input(&request(), MARKER, SOURCE, TARGET));
        assert!(!matches_probe_input(&request(), MARKER, TARGET, SOURCE));
        assert!(!matches_probe_input(
            &request(),
            "CCSWITCH_ROUTE_PROBE_wrong",
            SOURCE,
            TARGET
        ));
        let mut stale = request();
        stale["input"]
            .as_array_mut()
            .unwrap()
            .push(json!({"role":"user","content":"unrelated turn"}));
        assert!(!matches_probe_input(&stale, MARKER, SOURCE, TARGET));
    }

    fn fixture() -> (tempfile::TempDir, PathBuf, crate::provider::Provider) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        fs::write(
            &path,
            serde_json::to_vec(&json!({
                "version":1,"enabled":true,"run_id":uuid::Uuid::new_v4().to_string(),
                "source_task":SOURCE,"target_task":TARGET,"marker":MARKER,
                "provider_id":"synthetic-api","expires_ms":now_ms()+119_000
            }))
            .unwrap(),
        )
        .unwrap();
        let p = crate::provider::Provider::with_id(
            "synthetic-api".into(),
            "synthetic".into(),
            json!({}),
            None,
        );
        (dir, path, p)
    }

    fn completed() -> Value {
        json!({"type":"response.completed","response":{
            "id":"resp_fixture","status":"completed","output":[
                {"type":"reasoning","encrypted_content":"SYNTHETIC_SECRET"},
                {"type":"message","role":"assistant","content":[
                    {"type":"output_text","text":MARKER}
                ]}
            ],"Authorization":"SYNTHETIC_SECRET"
        }})
    }

    #[test]
    fn route_probe_no_file_for_unrelated_disabled_expired_or_wrong_provider() {
        let (dir, path, mut provider) = fixture();
        let registry = Mutex::new(None);
        let up = (
            "https://provider.example/v1/responses",
            "http",
            "native_responses",
        );
        assert!(begin_at(path.clone(), &json!({"input":[]}), &provider, up, &registry).is_none());
        provider.id = "other-api".into();
        assert!(begin_at(path.clone(), &request(), &provider, up, &registry).is_none());
        provider.id = "synthetic-api".into();
        let mut cfg: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        cfg["enabled"] = json!(false);
        fs::write(&path, cfg.to_string()).unwrap();
        assert!(begin_at(path.clone(), &request(), &provider, up, &registry).is_none());
        cfg["enabled"] = json!(true);
        cfg["expires_ms"] = json!(now_ms() - 1);
        fs::write(&path, cfg.to_string()).unwrap();
        assert!(begin_at(path, &request(), &provider, up, &registry).is_none());
        assert!(!dir.path().join("events.jsonl").exists());
    }

    #[test]
    fn route_probe_ws_summaries_drop_secrets_and_preserve_terminal_identity() {
        let (dir, path, provider) = fixture();
        let registry = Mutex::new(None);
        let mut req = request();
        req["Authorization"] = json!("SYNTHETIC_SECRET");
        req["history"] = json!("SYNTHETIC_SECRET");
        let probe = begin_at(
            path,
            &req,
            &provider,
            (
                "wss://user:SYNTHETIC_SECRET@provider.example/v1/responses?key=SYNTHETIC_SECRET",
                "ws",
                "native_responses",
            ),
            &registry,
        )
        .unwrap();
        probe.note("downstream_upgrade");
        probe.note("upstream_upgrade");
        probe.note("request_sent");
        probe.ws_text(&completed().to_string());
        drop(probe);
        let text = fs::read_to_string(dir.path().join("events.jsonl")).unwrap();
        assert!(!text.contains("SYNTHETIC_SECRET"));
        assert!(!text.contains("user:"));
        assert!(!text.contains(MARKER));
        let events: Vec<Value> = text
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        let terminal = events.iter().find(|e| e["phase"] == "terminal").unwrap();
        assert_eq!(terminal["detail"]["marker_exact"], true);
        assert_eq!(
            terminal["detail"]["response_id_sha256"],
            digest("resp_fixture")
        );
        assert_eq!(events[0]["detail"]["route"]["path"], "/v1/responses");
    }

    #[test]
    fn route_probe_ws_terminal_above_http_buffer_limit_keeps_identity_without_secrets() {
        let (dir, path, provider) = fixture();
        let registry = Mutex::new(None);
        let probe = begin_at(
            path,
            &request(),
            &provider,
            (
                "wss://provider.example/v1/responses",
                "ws",
                "native_responses",
            ),
            &registry,
        )
        .unwrap();
        let mut terminal = completed();
        terminal["response"]["output"][0]["encrypted_content"] =
            json!("SYNTHETIC_SECRET".repeat(6000));
        let frame = terminal.to_string();
        assert!(frame.len() > MAX_FRAME && frame.len() < 1_048_576);
        probe.ws_text(&frame);
        drop(probe);
        let text = fs::read_to_string(dir.path().join("events.jsonl")).unwrap();
        assert!(!text.contains("SYNTHETIC_SECRET"));
        let events: Vec<Value> = text
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        let terminal = events
            .iter()
            .find(|e| e["phase"] == "terminal")
            .expect("bounded WS terminal must not be discarded at the HTTP buffer size");
        assert_eq!(
            terminal["detail"]["response_id_sha256"],
            digest("resp_fixture")
        );
        assert_eq!(terminal["detail"]["marker_exact"], true);
    }

    #[tokio::test]
    async fn route_probe_http_sse_observes_fragmented_bytes_without_changing_them() {
        let (dir, path, provider) = fixture();
        let probe = begin_at(
            path,
            &request(),
            &provider,
            (
                "https://provider.example/v1/responses",
                "http",
                "native_responses",
            ),
            &Mutex::new(None),
        )
        .unwrap();
        let bytes =
            format!("event: response.completed\r\ndata: {}\r\n\r\n", completed()).into_bytes();
        let chunks = bytes
            .chunks(7)
            .map(|b| Ok::<_, std::io::Error>(bytes::Bytes::copy_from_slice(b)))
            .collect::<Vec<_>>();
        let mut headers = http::HeaderMap::new();
        headers.insert("content-type", "text/event-stream".parse().unwrap());
        headers.insert("authorization", "SYNTHETIC_SECRET".parse().unwrap());
        let response = probe.wrap_http(ProxyResponse::streamed(
            http::StatusCode::OK,
            headers,
            futures::stream::iter(chunks),
        ));
        let chunks = response.bytes_stream().collect::<Vec<_>>().await;
        let returned: Vec<u8> = chunks
            .into_iter()
            .flat_map(|b| b.unwrap().to_vec())
            .collect();
        assert_eq!(returned, bytes);
        let text = fs::read_to_string(dir.path().join("events.jsonl")).unwrap();
        assert!(text.contains("\"marker_exact\":true"));
        assert!(!text.contains("SYNTHETIC_SECRET"));
    }

    #[test]
    fn route_probe_ws_observation_budget_stays_bounded_and_reports_safe_counts() {
        let (dir, path, provider) = fixture();
        let registry = Mutex::new(None);
        let probe = begin_at(
            path,
            &request(),
            &provider,
            (
                "wss://provider.example/v1/responses",
                "ws",
                "native_responses",
            ),
            &registry,
        )
        .unwrap();
        let too_large = "SYNTHETIC_SECRET".repeat(MAX_WS_BYTES / 16 + 1);
        assert!(too_large.len() > MAX_WS_BYTES);
        probe.ws_text(&too_large);
        probe.ws_text(&completed().to_string());
        drop(probe);
        let text = fs::read_to_string(dir.path().join("events.jsonl")).unwrap();
        assert!(!text.contains("SYNTHETIC_SECRET"));
        let events: Vec<Value> = text
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert!(!events.iter().any(|e| e["phase"] == "terminal"));
        let limit = events
            .iter()
            .find(|e| e["phase"] == "observation_limit")
            .unwrap();
        assert_eq!(limit["detail"]["reason"], "ws_total_bytes");
        assert_eq!(limit["detail"]["frame_bytes"], too_large.len());
        assert!(text.len() < MAX_FRAME);
    }

    #[test]
    fn route_probe_attempt_event_limits_and_stop_are_fail_closed() {
        let (dir, path, provider) = fixture();
        let registry = Mutex::new(None);
        let up = (
            "https://provider.example/v1/responses",
            "http",
            "native_responses",
        );
        let first = begin_at(path.clone(), &request(), &provider, up, &registry).unwrap();
        for _ in 0..3 {
            assert!(begin_at(path.clone(), &request(), &provider, up, &registry).is_some());
        }
        assert!(begin_at(path.clone(), &request(), &provider, up, &registry).is_none());
        for _ in 0..100 {
            first.note("request_sent");
        }
        let file = dir.path().join("events.jsonl");
        let before = fs::read(&file).unwrap();
        assert!(before.len() <= MAX_FRAME);
        assert!(String::from_utf8_lossy(&before).lines().count() <= MAX_EVENTS);
        let mut cfg: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        cfg["enabled"] = json!(false);
        fs::write(path, cfg.to_string()).unwrap();
        first.ws_text(&completed().to_string());
        assert_eq!(fs::read(file).unwrap(), before);
    }

    #[test]
    fn route_probe_unknown_paths_and_invalid_config_do_not_leak() {
        let r = route("https://name:SYNTHETIC_SECRET@provider.example/key/SYNTHETIC_SECRET?token=SYNTHETIC_SECRET").unwrap();
        assert!(!r.to_string().contains("SYNTHETIC_SECRET"));
        assert!(r["path"].as_str().unwrap().starts_with("sha256:"));
        let (_dir, path, _) = fixture();
        let mut c: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        c["Authorization"] = json!("SYNTHETIC_SECRET");
        fs::write(&path, c.to_string()).unwrap();
        assert!(load_config(&path).is_none());
    }
}
