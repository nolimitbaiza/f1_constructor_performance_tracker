/**
 * Express application: the REST API under /api plus the static front end
 * from docs/. Kept separate from index.js so tests can create the app with
 * a fake data loader and no network port.
 */
import express from "express";
import { fileURLToPath } from "node:url";
import {
  seasonStats,
  progression,
  profile,
  insights,
  csv,
  SCORING,
} from "../docs/analytics.js";

/** Reads ?scoring= (official by default); anything else is a 400 error. */
function scoringParam(req) {
  const value = req.query.scoring ?? "official";
  if (typeof value !== "string" || !SCORING.includes(value))
    throw Object.assign(new Error("scoring must be official or rescored"), {
      status: 400,
    });
  return value;
}

/**
 * Builds the app. `loadData` is an async function returning the dataset
 * (from PostgreSQL or the bundled JSON snapshot).
 */
export function createApp(loadData) {
  const app = express();
  app.disable("x-powered-by");
  // Load the dataset once per API request and share it via res.locals.
  app.use("/api", async (req, res, next) => {
    try {
      res.locals.data = await loadData();
      next();
    } catch (error) {
      next(error);
    }
  });
  /**
   * Validates the query string and returns { year, ids }.
   * Invalid input throws an error with status 400 (handled at the bottom).
   */
  function filters(req, data) {
    const allowed = new Set(["season", "constructors", "format", "scoring"]);
    if (Object.keys(req.query).some((k) => !allowed.has(k)))
      throw Object.assign(new Error("Unknown query parameter"), {
        status: 400,
      });
    const raw = req.query.season;
    if (raw !== undefined && (typeof raw !== "string" || !/^\d{4}$/.test(raw)))
      throw Object.assign(new Error("season must be a four-digit year"), {
        status: 400,
      });
    const year = raw === undefined ? data.seasons.at(-1).year : Number(raw);
    if (!data.seasons.some((s) => s.year === year))
      throw Object.assign(new Error("Season is not available"), {
        status: 400,
      });
    const input = req.query.constructors;
    if (
      input !== undefined &&
      (typeof input !== "string" || !/^\d+(,\d+)*$/.test(input))
    )
      throw Object.assign(
        new Error("constructors must be comma-separated numeric IDs"),
        { status: 400 },
      );
    const ids =
      input === undefined ? [] : [...new Set(input.split(",").map(Number))];
    if (
      ids.length > 10 ||
      ids.some((id) => !data.constructors.some((c) => c.id === id))
    )
      throw Object.assign(new Error("Choose up to 10 known constructors"), {
        status: 400,
      });
    if (req.query.format !== undefined && req.query.format !== "csv")
      throw Object.assign(new Error("format must be csv"), { status: 400 });
    return { year, ids, scoring: scoringParam(req) };
  }
  /* ---------- Routes ---------- */

  app.get("/api/health", (req, res) =>
    res.json({
      status: "ok",
      source: process.env.DATABASE_URL ? "postgresql" : "snapshot",
    }),
  );
  app.get("/api/data", (req, res) => res.json(res.locals.data));
  app.get("/api/seasons", (req, res) => res.json(res.locals.data.seasons));
  app.get("/api/constructors", (req, res) =>
    res.json(res.locals.data.constructors),
  );
  app.get("/api/constructors/:id", (req, res) => {
    if (
      !/^\d+$/.test(req.params.id) ||
      !res.locals.data.constructors.some((c) => c.id === Number(req.params.id))
    )
      return res.status(404).json({ error: "Constructor not found" });
    res.json(
      profile(res.locals.data, Number(req.params.id), scoringParam(req)),
    );
  });
  // These four routes share filters and return season statistics as JSON,
  // or as a CSV download when ?format=csv is given.
  app.get(
    ["/api/dashboard", "/api/standings", "/api/comparison", "/api/analytics"],
    (req, res) => {
      const data = res.locals.data;
      const { year, ids, scoring } = filters(req, data);
      if (req.path === "/api/comparison" && ids.length < 2)
        return res
          .status(400)
          .json({ error: "Select at least two constructors" });
      const teams = seasonStats(data, year, ids, scoring);
      if (req.query.format === "csv")
        return res
          .attachment(`constructors-${year}.csv`)
          .type("text/csv")
          .send(
            csv(teams, [
              "name",
              "season",
              "rank",
              "points",
              "wins",
              "podiums",
              "races",
              "average",
              "scoring",
              "officialRank",
              "officialPoints",
            ]),
          );
      res.json({
        season: data.seasons.find((s) => s.year === year),
        teams,
        progression: progression(
          data,
          year,
          ids.length ? ids : teams.slice(0, 5).map((t) => t.id),
          scoring,
        ),
        insights: insights(data, year, scoring),
      });
    },
  );
  // Unknown API routes return JSON 404s instead of the HTML front end.
  app.use("/api", (req, res) =>
    res.status(404).json({ error: "API endpoint not found" }),
  );
  // Serve the front end (index.html, app.js, data.json, ...).
  app.use(express.static(fileURLToPath(new URL("../docs", import.meta.url))));
  // Error handler: client errors keep their message; server errors are
  // logged and replaced with a generic message so no connection details leak.
  app.use((error, req, res, next) => {
    if (res.headersSent) return next(error);
    const status = error.status ?? 500;
    if (status >= 500) console.error(error.message);
    res.status(status).json({
      error:
        status >= 500
          ? "Unable to load data. Check the server connection and retry."
          : error.message,
    });
  });
  return app;
}
