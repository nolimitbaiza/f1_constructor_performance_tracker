import express from "express";
import { fileURLToPath } from "node:url";
import {
  seasonStats,
  progression,
  profile,
  insights,
  csv,
} from "../docs/analytics.js";

export function createApp(loadData) {
  const app = express();
  app.disable("x-powered-by");
  app.use("/api", async (req, res, next) => {
    try {
      res.locals.data = await loadData();
      next();
    } catch (error) {
      next(error);
    }
  });
  function filters(req, data) {
    const allowed = new Set(["season", "constructors", "format"]);
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
    return { year, ids };
  }
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
    res.json(profile(res.locals.data, Number(req.params.id)));
  });
  app.get(
    ["/api/dashboard", "/api/standings", "/api/comparison", "/api/analytics"],
    (req, res) => {
      const data = res.locals.data;
      const { year, ids } = filters(req, data);
      if (req.path === "/api/comparison" && ids.length < 2)
        return res
          .status(400)
          .json({ error: "Select at least two constructors" });
      const teams = seasonStats(data, year, ids);
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
            ]),
          );
      res.json({
        season: data.seasons.find((s) => s.year === year),
        teams,
        progression: progression(
          data,
          year,
          ids.length ? ids : teams.slice(0, 5).map((t) => t.id),
        ),
        insights: insights(data, year),
      });
    },
  );
  app.use("/api", (req, res) =>
    res.status(404).json({ error: "API endpoint not found" }),
  );
  app.use(express.static(fileURLToPath(new URL("../docs", import.meta.url))));
  app.use((error, req, res, next) => {
    if (res.headersSent) return next(error);
    const status = error.status ?? 500;
    if (status >= 500) console.error(error.message);
    res
      .status(status)
      .json({
        error:
          status >= 500
            ? "Unable to load data. Check the server connection and retry."
            : error.message,
      });
  });
  return app;
}
