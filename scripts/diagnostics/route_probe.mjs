// One-shot native Responses route proof. Never writes a raw rollout or response.
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import readline from "node:readline";
import { pathToFileURL } from "node:url";

export const sha = (s) => crypto.createHash("sha256").update(s).digest("hex");
const uuid = (s) =>
  typeof s === "string" &&
  /^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(s);
const markerValid = (s) => /^CCSWITCH_ROUTE_PROBE_[0-9a-f]{32}$/i.test(s ?? "");
export function validateConfig(c) {
  const keys = [
    "version",
    "enabled",
    "run_id",
    "source_task",
    "target_task",
    "marker",
    "provider_id",
    "expires_ms",
  ];
  if (Object.keys(c).some((k) => !keys.includes(k)))
    throw Error("invalid_config");
  if (
    c.version !== 1 ||
    !uuid(c.run_id) ||
    !uuid(c.source_task) ||
    !uuid(c.target_task) ||
    !markerValid(c.marker) ||
    typeof c.provider_id !== "string" ||
    !c.provider_id.length ||
    c.provider_id.length > 128 ||
    typeof c.enabled !== "boolean" ||
    !Number.isSafeInteger(c.expires_ms) ||
    c.expires_ms < 0
  )
    throw Error("invalid_config");
  return c;
}
export function probePrompt(c) {
  return (
    "Reply only " +
    c.marker +
    ". Target task: " +
    c.target_task +
    ". Do not use tools or perform any other action."
  );
}
export function matchesInput(s, c) {
  return (
    typeof s === "string" &&
    s.length <= 4096 &&
    s.trim().startsWith("<codex_delegation>") &&
    s.trim().endsWith("</codex_delegation>") &&
    s.includes("<source_thread_id>" + c.source_task + "</source_thread_id>") &&
    s.includes("Reply only " + c.marker + ".") &&
    s.includes("Target task: " + c.target_task + ".")
  );
}
function delegationText(p) {
  if (p.type === "function_call_output") return p.output;
  if (
    p.type === "message" &&
    p.role === "user" &&
    Array.isArray(p.content) &&
    p.content.length === 1
  )
    return p.content[0].text;
  return null;
}
export function sanitizeRoute(r) {
  if (
    !r ||
    !["http", "https", "ws", "wss"].includes(r.scheme) ||
    typeof r.host !== "string" ||
    !/^[a-z0-9.:[\]-]{1,253}$/i.test(r.host)
  )
    return null;
  const safe =
    /^(\/(codex\/)?(v1\/){0,2}(responses|messages|chat\/completions)|sha256:[0-9a-f]{64})$/;
  return {
    scheme: r.scheme,
    host: r.host,
    port: Number.isInteger(r.port) ? r.port : null,
    path: safe.test(r.path ?? "") ? r.path : "sha256:" + sha(String(r.path)),
  };
}

// Exported to exercise synthetic rollout fixtures without reading live files.
export function rolloutSummary(rows, c) {
  let meta = null,
    active = null,
    selected = null,
    duplicate = false;
  for (const r of rows) {
    const p = r.payload ?? {};
    if (r.type === "session_meta") meta = p.id;
    if (r.type === "event_msg" && p.type === "task_started") active = p.turn_id;
    const inputText = delegationText(p);
    if (r.type === "response_item" && matchesInput(inputText, c)) {
      if (selected) duplicate = true;
      selected = {
        task: meta,
        turn: p.internal_chat_message_metadata_passthrough?.turn_id ?? active,
        input_sha256: sha(inputText),
        response_id_sha256: null,
        marker_exact: false,
        text_bytes: 0,
        tool_calls: 0,
        completed: false,
      };
    }
    if (!selected || selected.completed || active !== selected.turn) continue;
    if (
      r.type === "response_item" &&
      ["function_call", "custom_tool_call"].includes(p.type)
    )
      selected.tool_calls++;
    if (
      r.type === "response_item" &&
      p.type === "message" &&
      p.role === "assistant" &&
      (p.phase == null || p.phase === "final_answer") &&
      (p.channel == null || p.channel === "final")
    ) {
      const text = (p.content ?? [])
        .filter((x) => x.type === "output_text")
        .map((x) => x.text ?? "")
        .join("");
      selected.text_bytes = Buffer.byteLength(text);
      selected.marker_exact = text.trim() === c.marker;
    }
    if (
      r.type === "token_usage_record" &&
      p.turn_id === selected.turn &&
      typeof p.response_id === "string" &&
      p.response_id.length > 0 &&
      p.response_id.length <= 256
    )
      selected.response_id_sha256 = sha(p.response_id);
    if (
      r.type === "event_msg" &&
      p.type === "task_complete" &&
      p.turn_id === selected.turn
    )
      selected.completed = true;
  }
  return selected && !duplicate ? selected : null;
}

export function evaluate(c, events, local, now = Date.now()) {
  validateConfig(c);
  const base = {
    version: 1,
    run_id: c.run_id,
    source_task: c.source_task,
    target_task: c.target_task,
    marker_sha256: sha(c.marker),
    status: "INCONCLUSIVE",
    reason: "no_target_route_evidence",
    ws_to_sse_fallback: false,
    attempts: [],
  };
  const result = (status, reason) => ({ ...base, status, reason });
  if (events.length > 64) return result("INCONCLUSIVE", "event_limit");
  const scoped = events.filter(
    (e) =>
      e.version === 1 &&
      e.run_id === c.run_id &&
      e.source_task === c.source_task &&
      e.target_task === c.target_task &&
      e.marker_sha256 === sha(c.marker),
  );
  let previous = -1,
    pid = null;
  const groups = new Map();
  for (const e of scoped) {
    if (
      !uuid(e.attempt_id) ||
      !Number.isInteger(e.pid) ||
      e.pid < 1 ||
      (pid !== null && e.pid !== pid) ||
      !Number.isInteger(e.seq) ||
      e.seq <= previous ||
      !Number.isFinite(e.ts_ms) ||
      e.ts_ms > c.expires_ms ||
      e.ts_ms < c.expires_ms - 120000
    )
      return result("INCONCLUSIVE", "invalid_event_sequence");
    previous = e.seq;
    pid = e.pid;
    const group = groups.get(e.attempt_id) ?? [];
    group.push(e);
    groups.set(e.attempt_id, group);
  }
  if (groups.size > 4) return result("INCONCLUSIVE", "attempt_limit");
  if (
    !local ||
    local.task !== c.target_task ||
    !uuid(local.turn) ||
    !local.input_sha256
  )
    return result(
      "INCONCLUSIVE",
      now >= c.expires_ms
        ? "expired_without_correlated_rollout"
        : "missing_target_rollout",
    );
  const candidates = [];
  base.target_turn_id = local.turn;
  base.response_id_sha256 = /^[a-f0-9]{64}$/.test(
    local.response_id_sha256 ?? "",
  )
    ? local.response_id_sha256
    : null;
  base.rollout_text_bytes = Number.isSafeInteger(local.text_bytes)
    ? local.text_bytes
    : null;
  base.rollout_marker_exact = local.marker_exact === true;
  for (const [id, group] of groups) {
    const selected = group.find((e) => e.phase === "selected");
    const d = selected?.detail;
    if (
      !d ||
      d.provider_id_sha256 !== sha(c.provider_id) ||
      d.matched_item_sha256 !== local.input_sha256 ||
      d.adapter !== "native_responses"
    )
      continue;
    const route = sanitizeRoute(d.route);
    if (
      !route ||
      !["http", "ws"].includes(d.upstream_transport) ||
      (d.upstream_transport === "ws") !== ["ws", "wss"].includes(route.scheme)
    )
      continue;
    const sent = group.find((e) => e.phase === "request_sent");
    const up = group.find((e) => e.phase === "upstream_upgrade"),
      down = group.find((e) => e.phase === "downstream_upgrade");
    const headers = group.find((e) => e.phase === "response_headers");
    const routed =
      d.upstream_transport === "ws"
        ? !!sent &&
          !!up &&
          !!down &&
          down.seq > selected.seq &&
          up.seq > down.seq &&
          sent.seq > up.seq
        : !!headers &&
          headers.seq > selected.seq &&
          Number.isInteger(headers.detail?.status) &&
          headers.detail.status >= 200 &&
          headers.detail.status < 300;
    const terminal = group.find(
      (e) =>
        e.phase === "terminal" &&
        e.detail?.response_id_sha256 === local.response_id_sha256,
    );
    const failed = group.some((e) => e.phase === "send_failed");
    const limit = group.some((e) => e.phase === "observation_limit");
    // Never spread untrusted metadata or exception text into result.json.
    base.attempts.push({
      attempt_id: id,
      pid,
      transport: d.upstream_transport,
      adapter: "native_responses",
      provider_id_sha256: sha(c.provider_id),
      route,
      ws_send_recorded: !!sent,
      http_response_received: !!headers,
      upstream_upgrade: !!up,
      downstream_upgrade: !!down,
      routed,
      send_failed: failed,
      terminal:
        terminal &&
        ["completed", "failed", "incomplete"].includes(
          terminal.detail?.terminal,
        )
          ? terminal.detail.terminal
          : null,
    });
    candidates.push({
      selected,
      group,
      d,
      routed,
      terminal,
      failed,
      limit,
      headers,
    });
  }
  const found = candidates.filter((a) => a.routed && a.terminal);
  if (found.length !== 1 || !local.response_id_sha256)
    return result(
      "INCONCLUSIVE",
      now >= c.expires_ms
        ? "expired_without_complete_correlation"
        : "missing_unique_response_correlation",
    );
  const a = found[0],
    t = a.terminal.detail;
  if (
    a.limit ||
    a.headers?.detail?.encoded ||
    (a.headers &&
      (a.headers.detail?.redirected !== false ||
        !sanitizeRoute(a.headers.detail?.final_route))) ||
    a.terminal.seq <=
      (a.headers ?? a.group.find((e) => e.phase === "request_sent")).seq ||
    local.completed !== true
  )
    return result("INCONCLUSIVE", "incomplete_observation");
  if (!["completed", "failed", "incomplete"].includes(t.terminal))
    return result("INCONCLUSIVE", "unknown_terminal");
  if (t.terminal !== "completed")
    return result("FAIL", "upstream_noncompleted");
  if (
    t.output_present !== true ||
    typeof t.marker_exact !== "boolean" ||
    !Number.isSafeInteger(t.text_bytes) ||
    t.text_bytes < 0
  )
    return result("INCONCLUSIVE", "missing_output_summary");
  if (!t.marker_exact || !local.marker_exact || local.tool_calls !== 0)
    return result("FAIL", "marker_response_mismatch");
  if (a.d.upstream_transport === "http" && a.headers?.detail?.sse === true) {
    base.ws_to_sse_fallback = candidates.some(
      (w) =>
        w.d.upstream_transport === "ws" &&
        w.failed &&
        w.group.some(
          (e) => e.phase === "send_failed" && e.seq < a.selected.seq,
        ),
    );
  }
  return result("PASS", "target_marker_via_observed_api_route");
}

async function readRollout(file, c) {
  if (fs.statSync(file).size > 256 * 1024 * 1024)
    throw Error("rollout_size_limit");
  const reader = readline.createInterface({
    input: fs.createReadStream(file, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });
  const kept = [];
  let header = null,
    start = null,
    capture = false,
    bytes = 0;
  for await (const line of reader) {
    if (line.length > 8 * 1024 * 1024) throw Error("rollout_line_limit");
    const r = JSON.parse(line),
      p = r.payload ?? {};
    if (r.type === "session_meta") header = r;
    if (r.type === "event_msg" && p.type === "task_started") start = r;
    if (r.type === "response_item" && matchesInput(delegationText(p), c)) {
      if (capture) throw Error("duplicate_probe_input");
      capture = true;
      if (header) kept.push(header);
      if (start) kept.push(start);
    }
    if (capture) {
      bytes += line.length;
      if (bytes > 2 * 1024 * 1024) throw Error("probe_turn_size_limit");
      kept.push(r);
      if (r.type === "event_msg" && p.type === "task_complete") break;
    }
  }
  return rolloutSummary(kept, c);
}

export async function main(argv) {
  const [cmd, ...args] = argv;
  const opts = Object.fromEntries(
    args.reduce((a, v, i) => {
      if (i % 2 === 0) a.push([v.replace(/^--/, ""), args[i + 1]]);
      return a;
    }, []),
  );
  if (cmd === "prepare") {
    const dir = path.resolve(
      opts.dir ?? "release/route-probe",
      crypto.randomUUID(),
    );
    const c = validateConfig({
      version: 1,
      enabled: false,
      run_id: path.basename(dir),
      source_task: opts.source,
      target_task: opts.target,
      marker: "CCSWITCH_ROUTE_PROBE_" + crypto.randomBytes(16).toString("hex"),
      provider_id: opts.provider,
      expires_ms: 0,
    });
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(
      path.join(dir, "config.json"),
      JSON.stringify(c, null, 2),
      { flag: "wx" },
    );
    fs.writeFileSync(path.join(dir, "prompt.txt"), probePrompt(c), {
      flag: "wx",
    });
    console.log(
      JSON.stringify({
        status: "PREPARED_DISABLED",
        config: path.join(dir, "config.json"),
      }),
    );
  } else if (["arm", "stop", "report"].includes(cmd)) {
    const file = path.resolve(opts.config ?? "");
    if (fs.statSync(file).size > 4096) throw Error("config_size_limit");
    const c = validateConfig(JSON.parse(fs.readFileSync(file, "utf8")));
    if (cmd === "arm") {
      if (
        c.expires_ms !== 0 ||
        fs.existsSync(path.join(path.dirname(file), "events.jsonl"))
      )
        throw Error("one_shot_already_used");
      c.enabled = true;
      c.expires_ms = Date.now() + 120000;
      fs.writeFileSync(file, JSON.stringify(c, null, 2));
      console.log(
        JSON.stringify({ status: "ARMED", expires_ms: c.expires_ms }),
      );
    } else if (cmd === "stop") {
      c.enabled = false;
      fs.writeFileSync(file, JSON.stringify(c, null, 2));
      console.log(JSON.stringify({ status: "DISABLED" }));
    } else {
      const eventPath = path.join(path.dirname(file), "events.jsonl");
      let events = [],
        local = null,
        result;
      try {
        if (fs.existsSync(eventPath)) {
          if (fs.statSync(eventPath).size > 65536)
            throw Error("event_size_limit");
          events = fs
            .readFileSync(eventPath, "utf8")
            .split("\n")
            .filter(Boolean)
            .map((s) => JSON.parse(s));
        }
        if (opts.rollout) local = await readRollout(opts.rollout, c);
        result = evaluate(c, events, local);
      } catch {
        result = {
          version: 1,
          run_id: c.run_id,
          status: "INCONCLUSIVE",
          reason: "invalid_or_incomplete_evidence",
        };
      }
      fs.writeFileSync(
        path.join(path.dirname(file), "result.json"),
        JSON.stringify(result, null, 2),
      );
      console.log(JSON.stringify(result));
    }
  } else {
    console.log(
      "route_probe.mjs prepare --source UUID --target UUID --provider ID [--dir PATH]\nroute_probe.mjs arm|stop --config PATH\nroute_probe.mjs report --config PATH --rollout PATH",
    );
  }
}
if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href
) {
  main(process.argv.slice(2)).catch(() => {
    // Exception messages may include raw JSON/credentials. Never echo them.
    console.error(
      JSON.stringify({
        version: 1,
        status: "INCONCLUSIVE",
        reason: "invalid_or_unreadable_evidence",
      }),
    );
    process.exitCode = 2;
  });
}
