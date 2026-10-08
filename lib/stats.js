/**
 * Small helper every service uses to count what it does.
 * - Keeps counters (messages, readings, database operations, busy time).
 * - Every few seconds it saves them to the "service_stats" collection so
 *   tools/collect.js can read the numbers from all instances.
 * - Also serves them on a tiny RESTful endpoint: GET /health and GET /stats.
 */
const http = require("http");
const os = require("os");

function makeStats({ db, service, flushMs = 2000, port = 0 }) {
  const instanceId = `${service}-${os.hostname()}-${process.pid}`;
  const s = {
    _id: instanceId,
    service,
    startedAt: new Date().toISOString(),
    updatedAt: null,
    messages: 0,
    readings: 0,
    dbOps: 0,
    busyMs: 0,
    errors: 0,
  };

  function snapshot() {
    return { ...s, updatedAt: new Date().toISOString() };
  }

  async function flush() {
    try {
      await db.collection("service_stats").replaceOne({ _id: instanceId }, snapshot(), { upsert: true });
    } catch (e) {
      console.error(`[${service}] could not save stats:`, e.message);
    }
  }

  const timer = setInterval(flush, flushMs);
  let server = null;
  if (port) {
    server = http.createServer((req, res) => {
      res.setHeader("Content-Type", "application/json");
      if (req.url === "/health") return res.end(JSON.stringify({ status: "ok", service }));
      if (req.url === "/stats") return res.end(JSON.stringify(snapshot()));
      res.statusCode = 404;
      res.end(JSON.stringify({ error: "not found" }));
    });
    server.listen(port);
  }

  return {
    s,
    flush,
    // Wraps a database call so it is counted.
    dbOp: async (fn) => {
      s.dbOps++;
      return fn();
    },
    // Wraps a message handler so we know how long it was busy.
    timed: async (fn) => {
      const t = process.hrtime.bigint();
      try {
        return await fn();
      } catch (e) {
        s.errors++;
        console.error(`[${service}] error:`, e.message);
      } finally {
        s.busyMs += Number(process.hrtime.bigint() - t) / 1e6;
      }
    },
    async stop() {
      clearInterval(timer);
      await flush();
      if (server) server.close();
    },
  };
}

module.exports = { makeStats };
