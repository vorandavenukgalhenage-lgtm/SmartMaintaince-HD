// Node RED function node: "Edge Batcher"
// It is inbetween "Filter and Aggregate" and the mqtt out node.
// BATCH_SIZE = 1 means batching is off (baseline behaviour).
const BATCH_SIZE = 6;        // send after this many readings
const MAX_WAIT_MS = 5000;    // or when the oldest reading is this old

const now = Date.now();
const p = msg.payload;
const machine = p.machine_id;

let batches = flow.get("batches") || {};
let b = batches[machine] || { readings: [], first: now };

if (b.readings.length === 0) b.first = now;
b.readings.push({
    machine_id: p.machine_id,
    sensor_type: p.sensor_type,
    timestamp: p.timestamp,
    raw_value: p.raw_value,
    aggregated_value: p.aggregated_value
});

if (BATCH_SIZE <= 1) {
    batches[machine] = { readings: [], first: now };
    flow.set("batches", batches);
    return msg; // unchanged single message, original topic
}

if (b.readings.length >= BATCH_SIZE || (now - b.first) >= MAX_WAIT_MS) {
    const out = {
        topic: "processed/" + machine + "/batch",
        payload: { machine_id: machine, batch: true, count: b.readings.length, readings: b.readings }
    };
    batches[machine] = { readings: [], first: now };
    flow.set("batches", batches);
    return out;
}

batches[machine] = b;
flow.set("batches", batches);
return null; // keep collecting
