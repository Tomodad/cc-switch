// Synthetic fixtures only. Run explicitly with node --test; not Vitest discovery.
import test from "node:test";
import assert from "node:assert/strict";
import {
  evaluate,
  sha,
  rolloutSummary,
  probePrompt,
  validateConfig,
  main,
} from "./route_probe.mjs";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
const c = {
  version: 1,
  enabled: true,
  run_id: "30000000-0000-4000-8000-000000000003",
  source_task: "10000000-0000-4000-8000-000000000001",
  target_task: "20000000-0000-4000-8000-000000000002",
  marker: "CCSWITCH_ROUTE_PROBE_0123456789abcdef0123456789abcdef",
  provider_id: "synthetic-api",
  expires_ms: 121000,
};
const input =
  "<codex_delegation>\n<source_thread_id>" +
  c.source_task +
  "</source_thread_id>\n<input>" +
  probePrompt(c) +
  "</input>\n</codex_delegation>";
const local = {
  task: c.target_task,
  turn: "40000000-0000-4000-8000-000000000004",
  completed: true,
  marker_exact: true,
  text_bytes: c.marker.length,
  tool_calls: 0,
  input_sha256: sha(input),
  response_id_sha256: sha("resp_fixture"),
};
const attempt = "50000000-0000-4000-8000-000000000005";
function fixture(transport = "http", id = attempt) {
  const phases = [
    [
      "selected",
      {
        provider_id_sha256: sha(c.provider_id),
        route: {
          scheme: transport === "ws" ? "wss" : "https",
          host: "provider.example",
          port: 443,
          path: "/v1/responses",
        },
        upstream_transport: transport,
        adapter: "native_responses",
        input_items: 1,
        matched_item_sha256: sha(input),
      },
    ],
    ...(transport === "ws"
      ? [
          ["downstream_upgrade", {}],
          ["upstream_upgrade", {}],
        ]
      : []),
    ...(transport === "ws" ? [["request_sent", {}]] : []),
    ...(transport === "http"
      ? [
          [
            "response_headers",
            {
              status: 200,
              sse: true,
              encoded: false,
              redirected: false,
              final_route: {
                scheme: "https",
                host: "provider.example",
                port: 443,
                path: "/v1/responses",
              },
            },
          ],
        ]
      : []),
    [
      "terminal",
      {
        terminal: "completed",
        response_id_sha256: sha("resp_fixture"),
        output_present: true,
        text_bytes: c.marker.length,
        marker_exact: true,
        other_items: 0,
      },
    ],
    ["closed", {}],
  ];
  return phases.map(([phase, detail], seq) => ({
    version: 1,
    pid: 1234,
    run_id: c.run_id,
    source_task: c.source_task,
    target_task: c.target_task,
    marker_sha256: sha(c.marker),
    attempt_id: id,
    seq,
    ts_ms: 2000 + seq,
    phase,
    detail,
  }));
}
for (const t of ["http", "ws"])
  test("synthetic " + t + " proof + target rollout passes", () => {
    assert.equal(evaluate(c, fixture(t), local, 5000).status, "PASS");
  });
test("local Upgrade alone never passes", () => {
  assert.equal(
    evaluate(c, fixture("ws").slice(0, 2), local, 5000).status,
    "INCONCLUSIVE",
  );
});
test("unrelated concurrent task cannot supply proof", () => {
  assert.equal(
    evaluate(
      c,
      fixture().map((x) => ({ ...x, target_task: c.source_task })),
      local,
      5000,
    ).status,
    "INCONCLUSIVE",
  );
});
test("missing response correlation never passes", () => {
  assert.equal(
    evaluate(c, fixture(), { ...local, response_id_sha256: null }, 5000).status,
    "INCONCLUSIVE",
  );
});
test("routed empty completion fails", () => {
  const events = fixture();
  events.find((x) => x.phase === "terminal").detail.marker_exact = false;
  assert.equal(
    evaluate(c, events, { ...local, marker_exact: false, text_bytes: 0 }, 5000)
      .status,
    "FAIL",
  );
});
test("timeout without target route is inconclusive", () => {
  assert.equal(evaluate(c, [], null, 122000).status, "INCONCLUSIVE");
});
test("correlated WS failure then HTTP SSE succeeds", () => {
  const ws = fixture("ws").slice(0, 2);
  ws.push({ ...ws[0], seq: 2, ts_ms: 2002, phase: "send_failed", detail: {} });
  const http = fixture("http", "60000000-0000-4000-8000-000000000006").map(
    (x, i) => ({ ...x, seq: i + 3, ts_ms: 3000 + i }),
  );
  const r = evaluate(c, [...ws, ...http], local, 5000);
  assert.equal(r.status, "PASS");
  assert.equal(r.ws_to_sse_fallback, true);
});
test("unexpected sensitive fields are never echoed", () => {
  const e = fixture();
  e[0].Authorization = "SYNTHETIC_SECRET_DO_NOT_PERSIST";
  e[0].detail.route.query = "SYNTHETIC_SECRET_DO_NOT_PERSIST";
  e[0].detail.route.userinfo = "SYNTHETIC_SECRET_DO_NOT_PERSIST";
  e[0].detail.prompt = "SYNTHETIC_SECRET_DO_NOT_PERSIST";
  assert.ok(
    !JSON.stringify(evaluate(c, e, local, 5000)).includes("SYNTHETIC_SECRET"),
  );
});
test("rollout identifies delegation and final response ID", () => {
  const rows = [
    { type: "session_meta", payload: { id: c.target_task } },
    {
      type: "event_msg",
      payload: { type: "task_started", turn_id: local.turn },
    },
    {
      type: "response_item",
      payload: { type: "function_call_output", output: input },
    },
    {
      type: "response_item",
      payload: {
        type: "message",
        role: "assistant",
        content: [{ type: "output_text", text: c.marker }],
      },
    },
    {
      type: "token_usage_record",
      payload: { turn_id: local.turn, response_id: "resp_fixture" },
    },
    {
      type: "event_msg",
      payload: { type: "task_complete", turn_id: local.turn },
    },
  ];
  const r = rolloutSummary(rows, c);
  assert.equal(r.marker_exact, true);
  assert.equal(r.response_id_sha256, sha("resp_fixture"));
  assert.equal(r.completed, true);
});
test("wrong selected provider cannot pass", () => {
  const e = fixture();
  e[0].detail.provider_id_sha256 = sha("different-provider");
  assert.equal(evaluate(c, e, local, 5000).status, "INCONCLUSIVE");
});
test("different response ID cannot borrow another task result", () => {
  assert.equal(
    evaluate(c, fixture(), { ...local, response_id_sha256: sha("other") }, 5000)
      .status,
    "INCONCLUSIVE",
  );
});
test("mixed emitter PIDs cannot pass", () => {
  const e = fixture();
  e[1].pid = 9999;
  assert.equal(evaluate(c, e, local, 5000).status, "INCONCLUSIVE");
});
test("expired or oversized record sets cannot pass", () => {
  const e = fixture().map((x) => ({ ...x, ts_ms: 122000 }));
  assert.equal(evaluate(c, e, local, 122000).status, "INCONCLUSIVE");
  assert.equal(
    evaluate(c, Array(65).fill(fixture()[0]), local, 5000).status,
    "INCONCLUSIVE",
  );
});
test("HTTP error and missing upstream WS upgrade cannot pass", () => {
  const h = fixture();
  h.find((x) => x.phase === "response_headers").detail.status = 401;
  assert.equal(evaluate(c, h, local, 5000).status, "INCONCLUSIVE");
  assert.equal(
    evaluate(
      c,
      fixture("ws").filter((x) => x.phase !== "upstream_upgrade"),
      local,
      5000,
    ).status,
    "INCONCLUSIVE",
  );
});
test("late WS failure does not manufacture fallback", () => {
  const http = fixture();
  const ws = fixture("ws", "60000000-0000-4000-8000-000000000006").slice(0, 1);
  ws.push({ ...ws[0], phase: "send_failed", detail: {} });
  const e = [
    ...http,
    ...ws.map((x, i) => ({ ...x, seq: 10 + i, ts_ms: 3000 + i })),
  ];
  const r = evaluate(c, e, local, 5000);
  assert.equal(r.status, "PASS");
  assert.equal(r.ws_to_sse_fallback, false);
});
test("config with unexpected secret fields is rejected before rewriting", () => {
  assert.throws(
    () => validateConfig({ ...c, Authorization: "SYNTHETIC_SECRET" }),
    /invalid_config/,
  );
});
test("HTTP response receipt is not labeled as a WS send event", () => {
  const r = evaluate(c, fixture(), local, 5000);
  assert.equal(r.status, "PASS");
  assert.equal(r.attempts[0].ws_send_recorded, false);
  assert.equal(r.attempts[0].http_response_received, true);
});
test("HTTP timeout without headers does not imply request never left", () => {
  const e = fixture().filter(
    (x) => !["response_headers", "terminal"].includes(x.phase),
  );
  assert.equal(evaluate(c, e, local, 122000).status, "INCONCLUSIVE");
});
test("published schema covers the minimized config and result keys", () => {
  const schema = JSON.parse(
    fs.readFileSync(
      new URL("./route_probe.schema.json", import.meta.url),
      "utf8",
    ),
  );
  const r = evaluate(c, fixture("ws"), local, 5000);
  for (const key of Object.keys(c))
    assert.ok(key in schema.$defs.config.properties);
  for (const key of Object.keys(r))
    assert.ok(key in schema.$defs.result.properties);
  for (const key of Object.keys(r.attempts[0]))
    assert.ok(key in schema.$defs.attempt.properties);
});
test("hidden or commentary marker is not a final target reply", () => {
  const rows = [
    { type: "session_meta", payload: { id: c.target_task } },
    {
      type: "event_msg",
      payload: { type: "task_started", turn_id: local.turn },
    },
    {
      type: "response_item",
      payload: { type: "function_call_output", output: input },
    },
    {
      type: "response_item",
      payload: {
        type: "message",
        role: "assistant",
        phase: "commentary",
        content: [{ type: "output_text", text: c.marker }],
      },
    },
    {
      type: "event_msg",
      payload: { type: "task_complete", turn_id: local.turn },
    },
  ];
  assert.equal(rolloutSummary(rows, c).marker_exact, false);
});
test("unexpected redirect does not pass route proof", () => {
  const e = fixture();
  e.find((x) => x.phase === "response_headers").detail.redirected = true;
  assert.equal(evaluate(c, e, local, 5000).status, "INCONCLUSIVE");
});
test("CLI prepare/arm/stop uses only isolated synthetic files and never sends traffic", async () => {
  const temp = fs.mkdtempSync(
    path.join(os.tmpdir(), "cc-switch-route-probe-test-"),
  );
  try {
    await main([
      "prepare",
      "--source",
      c.source_task,
      "--target",
      c.target_task,
      "--provider",
      c.provider_id,
      "--dir",
      temp,
    ]);
    const dir = path.join(temp, fs.readdirSync(temp)[0]),
      file = path.join(dir, "config.json");
    assert.equal(JSON.parse(fs.readFileSync(file)).enabled, false);
    assert.ok(!fs.existsSync(path.join(dir, "events.jsonl")));
    await main(["arm", "--config", file]);
    await main(["stop", "--config", file]);
    assert.equal(JSON.parse(fs.readFileSync(file)).enabled, false);
    await assert.rejects(
      main(["arm", "--config", file]),
      /one_shot_already_used/,
    );
    await main(["report", "--config", file]);
    assert.equal(
      JSON.parse(fs.readFileSync(path.join(dir, "result.json"))).status,
      "INCONCLUSIVE",
    );
  } finally {
    // This exact mkdtemp result is owned by this test, not a user capture directory.
    if (
      !path.resolve(temp).startsWith(path.resolve(os.tmpdir()) + path.sep) ||
      !path.basename(temp).startsWith("cc-switch-route-probe-test-")
    )
      throw Error("unsafe_cleanup_target");
    fs.rmSync(temp, { recursive: true, force: true });
  }
});
