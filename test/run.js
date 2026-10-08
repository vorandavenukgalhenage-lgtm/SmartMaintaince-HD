// Integration tests: real Mosquitto broker and our real services + an in-memory fake database.
// Run with:  node test/run.js   (needs mosquitto installed, or set MQTT_URL to a running broker)
const assert = require("assert");
const { spawn } = require("child_process");
const mqtt = require("mqtt");
const { makeDb } = require("./fakedb");
const gateway = require("../services/gateway");
const anomaly = require("../services/anomaly");
const historian = require("../services/historian");
const alerting = require("../services/alerting");

const PORT = 18899;
const URL = process.env.MQTT_URL || `mqtt://127.0.0.1:${PORT}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitFor(cond, ms = 8000) { const t = Date.now(); while (Date.now() - t < ms) { if (await cond()) return; await sleep(50); } throw new Error("timeout waiting for condition"); }

async function startBroker() {
  if (process.env.MQTT_URL) return null;
  const conf = `listener ${PORT} 127.0.0.1\nallow_anonymous true\nmax_queued_messages 0\n`;
  require("fs").writeFileSync("/tmp/mosq_test.conf", conf);
  const p = spawn("mosquitto", ["-c", "/tmp/mosq_test.conf"], { stdio: "ignore" });
  await sleep(700);
  return p;
}
async function publisher() {
  const c = mqtt.connect(URL); await new Promise((r) => c.once("connect", r)); return c;
}
const raw = (c, m, s, v) => new Promise((r) => c.publish(`factory/${m}/${s}`, JSON.stringify({ machine_id: m, sensor_type: s, value: v, timestamp: "t" }), { qos: 1 }, r));

async function scenario({ batch, useIndex, anomalyCopies = 1 }) {
  const db = makeDb();
  const env = { BATCH_SIZE: String(batch), MAX_WAIT_MS: "800", USE_INDEX: String(useIndex), COOLDOWN_MS: "60000" };
  const svcs = [];
  for (let i = 0; i < anomalyCopies; i++) svcs.push(await anomaly.start({ mqttUrl: URL, db, env }));
  const hist = await historian.start({ mqttUrl: URL, db, env });
  const alert = await alerting.start({ mqttUrl: URL, db, env });
  const gw = await gateway.start({ mqttUrl: URL, db, env });
  await sleep(300);
  const pub = await publisher();
  // 60 normal readings for 3 machines x 1 sensor, then a big spike on M-001
  for (let i = 0; i < 40; i++) for (const m of ["M-001", "M-002", "M-003"]) await raw(pub, m, "vibration", 4.5 + (i % 5) * 0.02);
  await waitFor(() => svcs.reduce((a, s) => a + s.stats.s.readings, 0) >= 120);
  for (let i = 0; i < 6; i++) await raw(pub, "M-001", "vibration", 60 + i); // sustained fault
  await waitFor(() => db.collection("alerts").docs.length >= 1, 10000);
  await sleep(1500);
  const out = {
    db, gw, svcs, hist, alert,
    messages: gw.stats.s.messages,
    readingsIn: gw.stats.s.readings,
    readingsStored: db.collection("readings").docs.length,
    alerts: db.collection("alerts").docs.length,
    alertSeverities: db.collection("alerts").docs.map((d) => d.severity).sort().join(","),
    workorders: db.collection("workorders").docs.length,
    histInserts: db.collection("readings").calls.insertOne + db.collection("readings").calls.insertMany,
  };
  pub.end(true);
  await gw.stop(); for (const s of svcs) await s.stop(); await hist.stop(); await alert.stop();
  return out;
}

(async () => {
  const broker = await startBroker();
  try {
    // 1. baseline (no batching, no index)
    const a = await scenario({ batch: 1, useIndex: false });
    // 2. batching on, index on
    const b = await scenario({ batch: 6, useIndex: true });
    // 3. two copies of the anomaly service sharing the work
    const c = await scenario({ batch: 6, useIndex: true, anomalyCopies: 2 });

    // same data stored either way
    for (const r of [a, b, c]) assert.strictEqual(r.readingsStored, r.readingsIn, "all readings stored");
    // batching reduces the number of messages and database writes
    assert.ok(b.messages < a.messages / 4, `batching should cut messages (got ${a.messages} -> ${b.messages})`);
    assert.ok(b.histInserts < a.histInserts / 4, "historian should write far fewer times");
    // cooldown works the same with and without the index: a sustained fault gives ONE alert and ONE work order
    // (the cooldown is per severity, so a fault that grows from medium to high gives at most one alert of each)
    for (const r of [a, b, c]) {
      const sev = r.alertSeverities.split(",");
      assert.strictEqual(new Set(sev).size, sev.length, "no duplicate alerts of the same severity: " + r.alertSeverities);
      assert.ok(r.alerts >= 1 && r.alerts <= 2, "alerts " + r.alerts);
      assert.strictEqual(r.workorders, 1, "one work order");
    }
    assert.ok(a.db.collection("alerts").indexes.size === 0 && b.db.collection("alerts").indexes.has("machine_created_idx"), "index switch works");
    // shared subscription splits work between the two copies
    const [s1, s2] = c.svcs.map((s) => s.stats.s.messages);
    assert.ok(s1 > 0 && s2 > 0, `both copies must get messages (${s1}, ${s2})`);
    console.log("baseline : messages", a.messages, "historian writes", a.histInserts, "alerts", a.alertSeverities, "workorders", a.workorders);
    console.log("batched  : messages", b.messages, "historian writes", b.histInserts, "alerts", b.alertSeverities, "workorders", b.workorders);
    console.log("2 copies : anomaly messages per copy", s1, s2);
    console.log("ALL TESTS PASSED");
  } catch (e) {
    console.error("FAIL", e); process.exitCode = 1;
  } finally {
    if (broker) broker.kill();
    setTimeout(() => process.exit(process.exitCode || 0), 300);
  }
})();
