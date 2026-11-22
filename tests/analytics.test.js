import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import {
  seasonStats,
  profile,
  progression,
  insights,
  csv,
} from "../docs/analytics.js";
import { createApp } from "../server/app.js";
const data = JSON.parse(
  await readFile(new URL("../docs/data.json", import.meta.url)),
);

test("2024 standings, sprint-inclusive points and GP podium counts", () => {
  const teams = seasonStats(data, 2024);
  assert.equal(teams.length, 10);
  assert.deepEqual(
    teams.slice(0, 3).map((t) => [t.name, t.points, t.wins, t.podiums]),
    [
      ["McLaren", 666, 6, 21],
      ["Ferrari", 652, 5, 22],
      ["Red Bull", 589, 9, 18],
    ],
  );
  assert.equal(teams[0].earnedPoints, 666);
  assert.equal(
    teams.reduce((s, t) => s + t.wins, 0),
    24,
  );
  assert.equal(
    teams.reduce((s, t) => s + t.podiums, 0),
    72,
  );
});
test("exclusions do not count towards championship share", () => {
  const mclaren = seasonStats(data, 2007, [1])[0];
  assert.equal(mclaren.excluded, true);
  assert.equal(mclaren.points, 0);
  assert.equal(mclaren.share, 0);
  assert.equal(mclaren.wins, 8);
});
test("profiles exclude partial seasons from title and best/worst calculations", () => {
  assert.equal(profile(data, 6).titles, 16);
  assert.equal(profile(data, 131).titles, 8);
  assert.equal(profile(data, 1).seasons.at(-1).complete, false);
  assert.notEqual(profile(data, 1).best.season, 2025);
  assert.equal(data.seasons.at(-1).races, 15);
});
test("progression ends at official totals; absent entrants are gaps", () => {
  const p = progression(data, 2024, [1, 6, 25]);
  assert.equal(p.races.length, 24);
  assert.equal(p.series[0].values.at(-1), 666);
  assert.equal(p.series[1].values.at(-1), 652);
  assert.ok(p.series[2].values.every((v) => v === null));
  assert.equal(insights(data, 2024).length, 3);
});
test("CSV quotes strings, nulls and spreadsheet formulas safely", () => {
  assert.equal(
    csv([{ name: '=x,"y"', points: null }], ["name", "points"]),
    '"name","points"\r\n"\'=x,""y""",""',
  );
});
test("API filters, validation, missing routes and exports", async () => {
  const server = createApp(async () => data).listen(0, "127.0.0.1");
  await new Promise((resolve) => server.once("listening", resolve));
  const base = `http://127.0.0.1:${server.address().port}`;
  try {
    const result = await fetch(
      `${base}/api/comparison?season=2024&constructors=1,6`,
    );
    assert.equal(result.status, 200);
    assert.deepEqual(
      (await result.json()).teams.map((t) => t.points),
      [666, 652],
    );
    for (const path of [
      "dashboard?season=2024abc",
      "dashboard?season=2024&season=2023",
      "dashboard?constructors=99999",
      "comparison?constructors=1",
      "dashboard?format=xml",
      "dashboard?other=1",
    ])
      assert.equal((await fetch(`${base}/api/${path}`)).status, 400, path);
    assert.equal((await fetch(`${base}/api/constructors/nope`)).status, 404);
    assert.equal((await fetch(`${base}/api/missing`)).status, 404);
    const csvResponse = await fetch(
      `${base}/api/comparison?season=2024&constructors=1,6&format=csv`,
    );
    assert.match(csvResponse.headers.get("content-type"), /text\/csv/);
    assert.match(await csvResponse.text(), /"McLaren","2024","1","666"/);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});
test("database failures return a safe JSON error", async () => {
  const server = createApp(async () => {
    throw new Error("private connection details");
  }).listen(0, "127.0.0.1");
  await new Promise((resolve) => server.once("listening", resolve));
  try {
    const response = await fetch(
      `http://127.0.0.1:${server.address().port}/api/data`,
    );
    assert.equal(response.status, 500);
    assert.doesNotMatch(await response.text(), /private connection details/);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});
