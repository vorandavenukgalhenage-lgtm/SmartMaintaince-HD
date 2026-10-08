# SmartMaintaince HD: scalable Node.js microservices

Services (all Node.js, MQTT and MongoDB, one container each):
- gateway: edge layer, moving average and edge batching (same logic as the Node RED flow, see nodered/edge_batcher.js)
- anomaly: rolling baseline and z score, baseline kept in MongoDB, shared MQTT subscription so it can run as many copies
- historian: stores readings (insertMany for batches) and events
- alerting: alerts, work orders, 60 second cooldown query (index on or off with USE_INDEX)

Tools: tools/loadgen.js, tools/collect.js, tools/bench_index.js. Tests: npm test (needs mosquitto installed).
