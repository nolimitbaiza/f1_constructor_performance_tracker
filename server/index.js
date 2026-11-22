import { readFile } from "node:fs/promises";
import pg from "pg";
import { createApp } from "./app.js";

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
let cache;
let cachedAt = 0;
async function loadData() {
  if (!pool) return snapshot;
  if (cache && Date.now() - cachedAt < 30000) return cache;
  const client = await pool.connect();
  try {
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
const server = createApp(loadData).listen(
  Number(process.env.PORT ?? 3000),
  "127.0.0.1",
  () =>
    console.log(
      `Local: http://localhost:${server.address().port} (${pool ? "PostgreSQL" : "bundled snapshot"})`,
    ),
);
for (const signal of ["SIGINT", "SIGTERM"])
  process.on(signal, () =>
    server.close(async () => {
      await pool?.end();
      process.exit(0);
    }),
  );
