/**
 * Anomaly Detection service
 * Same algorithm as the Lambda version: rolling baseline (last 20 values), z score,
 * threshold 2.5, high severity above 4. The baseline lives in MongoDB so any number
 * of copies of this service can run (horizontal scaling).
 * It receives readings from a SHARED SUBSCRIPTION, so MQTT shares the messages
 * between all running copies instead of sending every message to every copy.
 * Handles both single readings and batch messages.
 */
const { connect, subscribe, serialHandler, publish } = require("../../lib/mqttutil");
const { makeStats } = require("../../lib/stats");

async function start({ mqttUrl, db, env = process.env }) {
  const Z = 2.5, WINDOW = 20;
  const stats = makeStats({ db, service: "anomaly", port: Number(env.STATS_PORT || 0) });
  const baselines = db.collection("baselines");
  const client = await connect(mqttUrl, `anomaly-${process.pid}-${Math.random().toString(16).slice(2, 6)}`);

  async function loadHistory(key) {
    const doc = await stats.dbOp(() => baselines.findOne({ _id: key }));
    return (doc && doc.history) || [];
  }
  async function saveHistory(key, history) {
    await stats.dbOp(() => baselines.replaceOne({ _id: key }, { _id: key, history }, { upsert: true }));
  }

  async function handle(topic, buf) {
    await stats.timed(async () => {
      const msg = JSON.parse(buf.toString());
      const readings = Array.isArray(msg.readings) ? msg.readings : [msg];
      stats.s.messages++;
      const cache = new Map(); // baselines loaded during this message (loaded and saved once)
      for (const r of readings) {
        if (r.aggregated_value === undefined) continue;
        stats.s.readings++;
        const key = `${r.machine_id}:${r.sensor_type}`;
        if (!cache.has(key)) cache.set(key, { history: await loadHistory(key), dirty: false });
        const entry = cache.get(key);
        const h = entry.history;
        let isAnomaly = false, z = 0;
        if (h.length >= 5) {
          const mean = h.reduce((a, b) => a + b, 0) / h.length;
          const variance = h.reduce((a, b) => a + (b - mean) ** 2, 0) / h.length;
          const sd = Math.sqrt(variance) || 0.0001;
          z = (r.aggregated_value - mean) / sd;
          isAnomaly = Math.abs(z) > Z;
        }
        if (!isAnomaly) {
          h.push(r.aggregated_value);
          if (h.length > WINDOW) h.shift();
          entry.dirty = true;
        } else {
          const zScore = Number(z.toFixed(2));
          await publish(client, "events/anomaly", {
            machine_id: r.machine_id,
            sensor_type: r.sensor_type,
            value: r.aggregated_value,
            z_score: zScore,
            severity: Math.abs(zScore) > 4 ? "high" : "medium",
            detected_at: new Date().toISOString(),
          });
        }
      }
      for (const [key, e] of cache) if (e.dirty) await saveHistory(key, e.history);
    });
  }

  client.on("message", serialHandler(handle));
  await subscribe(client, "$share/anomaly/processed/+/+", 1);
  console.log("[anomaly] running");
  return {
    stats,
    async stop() {
      await stats.stop();
      await new Promise((r) => client.end(false, {}, r));
    },
  };
}

module.exports = { start };
if (require.main === module) require("../../lib/main").run(start);
