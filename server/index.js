/**
 * Server entry point (npm start).
 *
 * With DATABASE_URL set, data is read from PostgreSQL and cached for 30
 * seconds. Without it, the bundled docs/data.json snapshot is used, so the
 * app runs without a database.
 */
import { readFile } from "node:fs/promises";
import pg from "pg";
import { createApp } from "./app.js";

// Connection pool, only created when a database is configured.
const pool = process.env.DATABASE_URL
  ? new pg.Pool({
      connectionString: process.env.DATABASE_URL,
      connectionTimeoutMillis: 5000,
      statement_timeout: 10000,
    })
  : null;
const snapshot = pool
  ? null
  : JSON.parse(
      await readFile(new URL("../docs/data.json", import.meta.url), "utf8"),
    );
// Most recent database read and when it was taken.
let cache;
let cachedAt = 0;
/** Returns the dataset, from the snapshot, the cache, or a fresh database read. */
async function loadData() {
  if (!pool) return snapshot;
  if (cache && Date.now() - cachedAt < 30000) return cache;
  const client = await pool.connect();
  try {
    // Read all tables inside one read-only transaction so they are consistent.
    await client.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    const data = {};
    for (const table of [
      "constructors",
      "seasons",
      "races",
      "results",
      "standings",
    ]) {
      const order = table === "seasons" ? " ORDER BY year" : "";
      data[table] = (
        await client.query(`SELECT * FROM f1.${table}${order}`)
      ).rows;
    }
    data.meta = (
      await client.query("SELECT data FROM f1.metadata WHERE id=1")
    ).rows[0]?.data;
    if (!data.meta || !data.seasons.length)
      throw new Error(
        "Import data with scripts/build_app_data.py --database first",
      );
    await client.query("COMMIT");
    cache = data;
    cachedAt = Date.now();
    return data;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}
// Listen on localhost only; this server is meant for local use.
const server = createApp(loadData).listen(
  Number(process.env.PORT ?? 3000),
  "127.0.0.1",
  () =>
    console.log(
      `Local: http://localhost:${server.address().port} (${pool ? "PostgreSQL" : "bundled snapshot"})`,
    ),
);
// Close the server and database pool cleanly on Ctrl+C or shutdown.
for (const signal of ["SIGINT", "SIGTERM"])
  process.on(signal, () =>
    server.close(async () => {
      await pool?.end();
      process.exit(0);
    }),
  );
