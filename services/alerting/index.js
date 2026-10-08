/**
 * Alerting and Work Order service
 * Listens to events/anomaly. For each anomaly it first checks the 60 second cooldown
 * (has this machine already had an alert / open work order recently?), which is a
 * database query on (machine_id, created_at). With USE_INDEX=false that query has to
 * look at every document in the collection. With USE_INDEX=true it uses the index.
 * High severity anomalies also create a work order and print a NOTIFY line
 * (this replaces the SNS email from the AWS version).
 */
const { connect, subscribe, serialHandler } = require("../../lib/mqttutil");
const { makeStats } = require("../../lib/stats");
const { applyIndexMode } = require("../../lib/indexes");

async function start({ mqttUrl, db, env = process.env }) {
  const COOLDOWN_MS = Number(env.COOLDOWN_MS || 60000);
  const useIndex = String(env.USE_INDEX || "false") === "true";
  const stats = makeStats({ db, service: "alerting", port: Number(env.STATS_PORT || 0) });
  const alerts = db.collection("alerts");
  const workorders = db.collection("workorders");
  const machines = db.collection("machines");
  await applyIndexMode(db, ["alerts", "workorders"], useIndex);
  const client = await connect(mqttUrl, `alerting-${process.pid}-${Math.random().toString(16).slice(2, 6)}`);
  const id = (p) => `${p}-${Date.now()}-${Math.floor(Math.random() * 100000)}`;

  async function handle(topic, buf) {
    await stats.timed(async () => {
      const a = JSON.parse(buf.toString());
      stats.s.messages++;
      const cutoff = new Date(Date.now() - COOLDOWN_MS).toISOString();

      const recentAlert = await stats.dbOp(() =>
        alerts.findOne({ machine_id: a.machine_id, created_at: { $gt: cutoff }, severity: a.severity })
      );
      if (recentAlert) return;

      const alert = {
        _id: id("ALT"),
        machine_id: a.machine_id,
        severity: a.severity,
        description: `${a.sensor_type} reading of ${a.value} is ${a.z_score} standard deviations from baseline`,
        created_at: new Date().toISOString(),
        status: "open",
      };
      await stats.dbOp(() => alerts.insertOne(alert));
      stats.s.readings++; // alerts created

      if (a.severity !== "high") return;
      console.log(`NOTIFY: ${alert.description} (${alert._id})`);

      const recentWo = await stats.dbOp(() =>
        workorders.findOne({ machine_id: a.machine_id, created_at: { $gt: cutoff }, sensor_type: a.sensor_type, status: "open" })
      );
      if (recentWo) return;
      const known = await stats.dbOp(() => machines.findOne({ _id: a.machine_id }));
      if (!known) await stats.dbOp(() => machines.insertOne({ _id: a.machine_id, name: a.machine_id, location: "auto registered" }));
      await stats.dbOp(() =>
        workorders.insertOne({
          _id: id("WO"),
          machine_id: a.machine_id,
          sensor_type: a.sensor_type,
          reason: `Anomalous ${a.sensor_type} reading (z=${a.z_score})`,
          status: "open",
          created_at: new Date().toISOString(),
        })
      );
    });
  }

  client.on("message", serialHandler(handle));
  await subscribe(client, "$share/alerting/events/anomaly", 1);
  console.log(`[alerting] running, USE_INDEX=${useIndex}`);
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
