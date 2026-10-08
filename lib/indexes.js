/**
 * Switches the cooldown index on or off, so the same code can be tested both ways.
 * USE_INDEX=true  -> compound index on (machine_id, created_at)
 * USE_INDEX=false -> only the default _id index (queries must scan the collection)
 */
const INDEX_NAME = "machine_created_idx";

async function applyIndexMode(db, collections, useIndex) {
  for (const name of collections) {
    const col = db.collection(name);
    if (useIndex) {
      await col.createIndex({ machine_id: 1, created_at: -1 }, { name: INDEX_NAME });
    } else {
      try {
        await col.dropIndex(INDEX_NAME);
      } catch (e) {
        /* index did not exist, fine */
      }
    }
  }
}

module.exports = { applyIndexMode, INDEX_NAME };
