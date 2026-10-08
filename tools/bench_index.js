/**
 * Benchmarks the cooldown query with and without the index at several collection sizes.
 *   node tools/bench_index.js            sizes 1000, 10000, 50000, 100000
 *   SIZES=1000,5000 RUNS=100 node tools/bench_index.js
 * Uses its own collection (bench_alerts), so it does not touch real data.
 */
const { MongoClient } = require("mongodb");
const MONGO_URL = process.env.MONGO_URL || "mongodb://localhost:27017";
const SIZES = (process.env.SIZES || "1000,10000,50000,100000").split(",").map(Number);
const RUNS = Number(process.env.RUNS || 200);
const INDEX = "machine_created_idx";

const median = (a) => [...a].sort((x, y) => x - y)[Math.floor(a.length / 2)];
const p95 = (a) => [...a].sort((x, y) => x - y)[Math.min(a.length - 1, Math.floor(a.length * 0.95))];

async function timeQueries(col, filter) {
  const times = [];
  for (let i = 0; i < 20; i++) await col.findOne(filter); // warm up
  for (let i = 0; i < RUNS; i++) {
    const t = process.hrtime.bigint();
    await col.findOne(filter);
    times.push(Number(process.hrtime.bigint() - t) / 1e6);
  }
  const ex = await col.find(filter).limit(1).explain("executionStats");
  const stage = JSON.stringify(ex.queryPlanner.winningPlan).includes("IXSCAN") ? "IXSCAN" : "COLLSCAN";
  return { median: median(times), p95: p95(times), docs: ex.executionStats.totalDocsExamined, stage };
}

(async () => {
  const mongo = await MongoClient.connect(MONGO_URL);
  const col = mongo.db(process.env.MONGO_DB || "smartmaintaince").collection("bench_alerts");
  await col.deleteMany({});
  await col.dropIndexes().catch(() => { });
  let have = 0;
  const cutoff = new Date(Date.now() - 60000).toISOString();
  const filter = { machine_id: "M-001", created_at: { $gt: cutoff }, severity: "high" };
  console.log("size,mode,plan,docsExamined,median_ms,p95_ms");
  for (const size of SIZES) {
    while (have < size) {
      const n = Math.min(10000, size - have);
      const docs = Array.from({ length: n }, (_, i) => ({
        machine_id: "M-" + String(((have + i) % 12) + 1).padStart(3, "0"),
        severity: "medium",
        created_at: new Date(Date.now() - 3600000 - (have + i) * 1000).toISOString(),
        status: "open",
      }));
      await col.insertMany(docs);
      have += n;
    }
    await col.dropIndex(INDEX).catch(() => { });
    let r = await timeQueries(col, filter);
    console.log(`${size},no index,${r.stage},${r.docs},${r.median.toFixed(3)},${r.p95.toFixed(3)}`);
    await col.createIndex({ machine_id: 1, created_at: -1 }, { name: INDEX });
    r = await timeQueries(col, filter);
    console.log(`${size},with index,${r.stage},${r.docs},${r.median.toFixed(3)},${r.p95.toFixed(3)}`);
  }
  await col.drop().catch(() => { });
  await mongo.close();
})();
