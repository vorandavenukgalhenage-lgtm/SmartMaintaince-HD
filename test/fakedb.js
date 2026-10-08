// A tiny in-memory stand in for the MongoDB driver (only the calls our services use).
// Lets the tests run without a MongoDB server.
function matches(doc, filter) {
  return Object.entries(filter).every(([k, v]) => {
    if (v && typeof v === "object" && "$gt" in v) return doc[k] > v.$gt;
    return doc[k] === v;
  });
}
class Col {
  constructor() { this.docs = []; this.indexes = new Set(); this.calls = { insertOne: 0, insertMany: 0, findOne: 0, replaceOne: 0 }; }
  async findOne(f) { this.calls.findOne++; return this.docs.find((d) => matches(d, f)) || null; }
  find(f = {}) { const r = this.docs.filter((d) => matches(d, f)); return { toArray: async () => r }; }
  async insertOne(d) { this.calls.insertOne++; this.docs.push({ ...d }); return {}; }
  async insertMany(a) { this.calls.insertMany++; this.docs.push(...a.map((d) => ({ ...d }))); return {}; }
  async replaceOne(f, d, o = {}) {
    this.calls.replaceOne++;
    const i = this.docs.findIndex((x) => matches(x, f));
    if (i >= 0) this.docs[i] = { ...d }; else if (o.upsert) this.docs.push({ ...d });
    return {};
  }
  async deleteMany() { this.docs = []; return {}; }
  async createIndex(spec, o) { this.indexes.add(o.name); return o.name; }
  async dropIndex(name) { if (!this.indexes.has(name)) throw new Error("index not found"); this.indexes.delete(name); }
}
function makeDb() {
  const cols = {};
  return { collection: (n) => (cols[n] = cols[n] || new Col()), cols };
}
module.exports = { makeDb };
