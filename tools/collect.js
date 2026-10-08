/**
 * Reads the counters every service saved in MongoDB and prints a summary.
 *   node tools/collect.js              show a summary now
 *   node tools/collect.js --reset      clear stats, readings, alerts, work orders, baselines, runs
 *   node tools/collect.js --wait 30000 wait until the anomaly services have processed 30000 readings
 *                                      (use after loadgen --burst) and print the drain time
 */
const { MongoClient } = require("mongodb");
const MONGO_URL = process.env.MONGO_URL || "mongodb://localhost:27017";
const arg = (n) => process.argv.indexOf(n);

async function summary(db) {
  const rows = await db.collection("service_stats").find({}).toArray();
  const by = {};
  for (const r of rows) {
    const g = (by[r.service] = by[r.service] || { instances: 0, messages: 0, readings: 0, dbOps: 0, busyMs: 0, errors: 0 });
    g.instances++; g.messages += r.messages; g.readings += r.readings; g.dbOps += r.dbOps; g.busyMs += r.busyMs; g.errors += r.errors;
  }
  console.log("service     instances  messages  readings  dbOps   busy(s)  errors");
  for (const [k, g] of Object.entries(by))
    console.log(`${k.padEnd(11)} ${String(g.instances).padStart(9)} ${String(g.messages).padStart(9)} ${String(g.readings).padStart(9)} ${String(g.dbOps).padStart(6)} ${(g.busyMs / 1000).toFixed(1).padStart(9)} ${String(g.errors).padStart(7)}`);
  return by;
}

(async () => {
  const mongo = await MongoClient.connect(MONGO_URL);
  const db = mongo.db(process.env.MONGO_DB || "smartmaintaince");
  if (arg("--reset") > -1) {
    for (const c of ["service_stats", "readings", "events", "alerts", "workorders", "baselines", "runs"])
      await db.collection(c).deleteMany({});
    console.log("Cleared. (Restart the services so their counters start from zero.)");
  } else if (arg("--wait") > -1) {
    const target = Number(process.argv[arg("--wait") + 1]);
    let run = null;
    for (let i = 0; i < 120 && !run; i++) { // wait up to 2 minutes for loadgen --burst to start
      run = await db.collection("runs").find({ type: "burst" }).sort({ t0: -1 }).limit(1).next();
      if (!run) { if (i === 0) console.log("Waiting for loadgen --burst to start..."); await new Promise((r) => setTimeout(r, 1000)); }
    }
    if (!run) throw new Error("No burst run found. Start loadgen --burst first.");
    const t0 = new Date(run.t0).getTime();
    console.log("Waiting for", target, "readings to be processed by the anomaly service...");
    for (;;) {
      const rows = await db.collection("service_stats").find({ service: "anomaly" }).toArray();
      const done = rows.reduce((a, r) => a + r.readings, 0);
      const last = Math.max(...rows.map((r) => new Date(r.updatedAt).getTime()));
      if (done >= target) {
        const secs = (last - t0) / 1000;
        console.log(`DONE: ${done} readings, ${rows.length} anomaly instance(s), drain time about ${secs.toFixed(1)} s, ${(done / secs).toFixed(0)} readings/s`);
        break;
      }
      await new Promise((r) => setTimeout(r, 1000));
    }
    await new Promise((r) => setTimeout(r, 2500)); // let all stats flush
    await summary(db);
  } else {
    await summary(db);
  }
  await mongo.close();
})();
