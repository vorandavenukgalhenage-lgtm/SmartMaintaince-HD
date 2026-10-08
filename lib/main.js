// Starts a service as its own process: connects to MongoDB, then calls start().
const { MongoClient } = require("mongodb");
async function run(start) {
  const mqttUrl = process.env.MQTT_URL || "mqtt://localhost:1883";
  const mongoUrl = process.env.MONGO_URL || "mongodb://localhost:27017";
  const mongo = await MongoClient.connect(mongoUrl);
  const db = mongo.db(process.env.MONGO_DB || "smartmaintaince");
  const svc = await start({ mqttUrl, db, env: process.env });
  const bye = async () => { await svc.stop(); await mongo.close(); process.exit(0); };
  process.on("SIGINT", bye);
  process.on("SIGTERM", bye);
}
module.exports = { run };
