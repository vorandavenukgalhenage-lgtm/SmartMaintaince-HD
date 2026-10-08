const mqtt = require("mqtt");

function connect(url, clientId) {
  return new Promise((resolve, reject) => {
    const client = mqtt.connect(url, { clientId, clean: true, reconnectPeriod: 1000 });
    client.once("connect", () => resolve(client));
    client.once("error", reject);
  });
}

function subscribe(client, topic, qos = 1) {
  return new Promise((resolve, reject) =>
    client.subscribe(topic, { qos }, (err, granted) => (err ? reject(err) : resolve(granted)))
  );
}

// Handles messages one at a time per service instance (keeps the baseline
// updates for one sensor in order inside that instance).
function serialHandler(fn) {
  let chain = Promise.resolve();
  return (topic, payload) => {
    chain = chain.then(() => fn(topic, payload)).catch((e) => console.error("handler error:", e.message));
  };
}

function publish(client, topic, obj, qos = 1) {
  return new Promise((resolve, reject) =>
    client.publish(topic, JSON.stringify(obj), { qos }, (err) => (err ? reject(err) : resolve()))
  );
}

module.exports = { connect, subscribe, serialHandler, publish };
