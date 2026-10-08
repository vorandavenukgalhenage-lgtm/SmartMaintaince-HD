/**
 * Edge Gateway service (fog/edge layer)
 * Does the same job as the Node RED flow "Filter and Aggregate" and "Edge Batcher":
 *   1. subscribes to raw readings   factory/<machine>/<sensor>
 *   2. smooths each sensor with a moving average of the last 5 readings
 *   3. EDGE BATCHING: instead of publishing every reading on its own, it groups
 *      the readings of one machine and publishes one message
 *      processed/<machine>/batch after BATCH_SIZE readings or MAX_WAIT_MS.
 * BATCH_SIZE=1 switches batching off (original behaviour, one message per reading).
 */
const { connect, subscribe, publish } = require("../../lib/mqttutil");
const { makeStats } = require("../../lib/stats");

async function start({ mqttUrl, db, env = process.env }) {
  const BATCH_SIZE = Number(env.BATCH_SIZE || 6);
  const MAX_WAIT_MS = Number(env.MAX_WAIT_MS || 5000);
  const WINDOW = 5;

  const stats = makeStats({ db, service: "gateway", port: Number(env.STATS_PORT || 0) });
  const client = await connect(mqttUrl, `gateway-${process.pid}-${Math.random().toString(16).slice(2, 6)}`);

  const windows = {}; // key -> last values
  const batches = {}; // machine -> { readings, first }

  async function flush(machine) {
    const b = batches[machine];
    if (!b || b.readings.length === 0) return;
    const readings = b.readings;
    batches[machine] = { readings: [], first: 0 };
    await publish(client, `processed/${machine}/batch`, {
      machine_id: machine,
      batch: true,
      count: readings.length,
      readings,
    });
    stats.s.messages++; // messages published
  }

  client.on("message", (topic, buf) => {
    stats.timed(async () => {
      const p = JSON.parse(buf.toString());
      const key = `${p.machine_id}:${p.sensor_type}`;
      const w = (windows[key] = windows[key] || []);
      w.push(p.value);
      if (w.length > WINDOW) w.shift();
      const avg = w.reduce((a, b) => a + b, 0) / w.length;
      const reading = {
        machine_id: p.machine_id,
        sensor_type: p.sensor_type,
        timestamp: p.timestamp,
        raw_value: p.value,
        aggregated_value: Number(avg.toFixed(2)),
      };
      stats.s.readings++; // readings received

      if (BATCH_SIZE <= 1) {
        await publish(client, `processed/${p.machine_id}/${p.sensor_type}`, reading);
        stats.s.messages++;
        return;
      }
      const b = (batches[p.machine_id] = batches[p.machine_id] || { readings: [], first: 0 });
      if (b.readings.length === 0) b.first = Date.now();
      b.readings.push(reading);
      if (b.readings.length >= BATCH_SIZE) await flush(p.machine_id);
    });
  });
  await subscribe(client, "factory/+/+", 1);

  // Timer flush: a quiet machine never keeps readings waiting longer than MAX_WAIT_MS.
  const timer = setInterval(() => {
    const now = Date.now();
    for (const m of Object.keys(batches)) {
      const b = batches[m];
      if (b.readings.length > 0 && now - b.first >= MAX_WAIT_MS) flush(m).catch(() => { });
    }
  }, Math.min(250, MAX_WAIT_MS));

  console.log(`[gateway] running, BATCH_SIZE=${BATCH_SIZE} MAX_WAIT_MS=${MAX_WAIT_MS}`);
  return {
    stats,
    async stop() {
      clearInterval(timer);
      for (const m of Object.keys(batches)) await flush(m).catch(() => { });
      await stats.stop();
      await new Promise((r) => client.end(false, {}, r));
    },
  };
}

module.exports = { start };
if (require.main === module) require("../../lib/main").run(start);
