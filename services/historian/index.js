/**
 * Historian service
 * Stores every processed reading in the "readings" collection and every event in "events".
 * A batch message is written with ONE insertMany call instead of one insertOne per reading.
 */
const { connect, subscribe, serialHandler } = require("../../lib/mqttutil");
const { makeStats } = require("../../lib/stats");

async function start({ mqttUrl, db, env = process.env }) {
  const stats = makeStats({ db, service: "historian", port: Number(env.STATS_PORT || 0) });
  const readingsCol = db.collection("readings");
  const eventsCol = db.collection("events");
  const client = await connect(mqttUrl, `historian-${process.pid}-${Math.random().toString(16).slice(2, 6)}`);

  async function handle(topic, buf) {
    await stats.timed(async () => {
      const msg = JSON.parse(buf.toString());
      stats.s.messages++;
      if (topic.startsWith("processed/")) {
        const list = Array.isArray(msg.readings) ? msg.readings : [msg];
        const docs = list.map((r) => ({ ...r, is_anomaly: false, stored_at: new Date().toISOString() }));
        stats.s.readings += docs.length;
        if (Array.isArray(msg.readings)) await stats.dbOp(() => readingsCol.insertMany(docs));
        else await stats.dbOp(() => readingsCol.insertOne(docs[0]));
      } else if (topic.startsWith("events/")) {
        await stats.dbOp(() => eventsCol.insertOne({ topic, ...msg, logged_at: new Date().toISOString() }));
      }
    });
  }

  client.on("message", serialHandler(handle));
  await subscribe(client, "$share/historian/processed/+/+", 1);
  await subscribe(client, "$share/historian-events/events/#", 1);
  console.log("[historian] running");
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
