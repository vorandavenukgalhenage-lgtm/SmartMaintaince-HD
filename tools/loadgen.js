/**
 * Load generator: pretends to be many machines publishing raw readings.
 * Same topic and payload as your simulator:  factory/<machine>/<sensor>
 *   { machine_id, sensor_type, value, timestamp }
 *
 * Steady mode (default):  MACHINES=12 RATE_MS=2000 DURATION_S=600 node tools/loadgen.js
 * Burst mode:             node tools/loadgen.js --burst 30000
 *   (publishes 30000 readings as fast as possible; used to measure how fast the
 *    processing services can drain a backlog)
 */
const mqtt = require("mqtt");
const { MongoClient } = require("mongodb");

const MQTT_URL = process.env.MQTT_URL || "mqtt://localhost:1883";
const MONGO_URL = process.env.MONGO_URL || "mongodb://localhost:27017";
const MACHINES = Number(process.env.MACHINES || 12);
const RATE_MS = Number(process.env.RATE_MS || 2000);
const DURATION_S = Number(process.env.DURATION_S || 600);
const SENSORS = ["vibration", "temperature", "current"];
const BASE = { vibration: 4.5, temperature: 45, current: 12 };

const burstIdx = process.argv.indexOf("--burst");
const BURST = burstIdx > -1 ? Number(process.argv[burstIdx + 1]) : 0;

const rand = (s) => BASE[s] * (1 + (Math.random() - 0.5) * 0.1);
const id = (n) => "M-" + String(n).padStart(3, "0");
const reading = (m, s, spike) => ({
  machine_id: m,
  sensor_type: s,
  value: Number((spike ? BASE[s] * 2.5 : rand(s)).toFixed(2)),
  timestamp: new Date().toISOString(),
});

(async () => {
  const client = mqtt.connect(MQTT_URL, { clientId: "loadgen-" + process.pid });
  await new Promise((r) => client.once("connect", r));
  const pub = (p) =>
    new Promise((res) => client.publish(`factory/${p.machine_id}/${p.sensor_type}`, JSON.stringify(p), { qos: 1 }, res));

  if (BURST) {
    const mongo = await MongoClient.connect(MONGO_URL);
    const db = mongo.db(process.env.MONGO_DB || "smartmaintaince");
    const t0 = Date.now();
    await db.collection("runs").insertOne({ type: "burst", readings: BURST, machines: MACHINES, t0: new Date(t0).toISOString() });
    console.log(`BURST START ${new Date(t0).toISOString()}: publishing ${BURST} readings from ${MACHINES} machines`);
    const pending = [];
    for (let i = 0; i < BURST; i++) {
      const m = id((i % MACHINES) + 1), s = SENSORS[i % SENSORS.length];
      pending.push(pub(reading(m, s, Math.random() < 0.002)));
      if (pending.length >= 200) await Promise.all(pending.splice(0));
    }
    await Promise.all(pending);
    console.log(`Published all ${BURST} readings in ${((Date.now() - t0) / 1000).toFixed(1)} s`);
    await mongo.close();
    client.end();
    return;
  }

  console.log(`START ${new Date().toISOString()}: ${MACHINES} machines x ${SENSORS.length} sensors every ${RATE_MS} ms for ${DURATION_S} s`);
  const timer = setInterval(() => {
    for (let n = 1; n <= MACHINES; n++) for (const s of SENSORS) pub(reading(id(n), s, Math.random() < 0.002));
  }, RATE_MS);
  setTimeout(() => {
    clearInterval(timer);
    console.log(`END ${new Date().toISOString()}`);
    client.end();
  }, DURATION_S * 1000);
})();
