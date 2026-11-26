/**
 * Tests for the shared statistics (docs/analytics.js) and the REST API
 * (server/app.js), using the real bundled dataset. Run with: npm test
 */
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
test("2025 rules re-score the 2025 season to exactly the official table", () => {
  const official = seasonStats(data, 2025);
  const rescored = seasonStats(data, 2025, [], "rescored");
  assert.equal(rescored.length, official.length);
  for (const team of official) {
    const same = rescored.find((t) => t.id === team.id);
    assert.equal(same.points, team.points, team.name);
    assert.equal(same.rank, team.rank, team.name);
  }
});
test("re-scoring compares eras like with like", () => {
  const [mclaren, ferrari] = seasonStats(data, 1988, [], "rescored");
  assert.deepEqual(
    [mclaren.name, mclaren.points, mclaren.rank, mclaren.officialPoints],
    ["McLaren", 576, 1, 199],
  );
  assert.deepEqual([ferrari.name, ferrari.points], ["Ferrari", 239]);
  // 2023: only the fastest-lap points (no longer awarded) disappear.
  assert.equal(seasonStats(data, 2023, [9], "rescored")[0].points, 849);
  // 1958: only each team's best car used to score, so the champion changes.
  const [champion] = seasonStats(data, 1958, [], "rescored");
  assert.deepEqual(
    [champion.name, champion.rank, champion.officialRank],
    ["Ferrari", 1, 2],
  );
  assert.deepEqual(insights(data, 1958, "rescored").at(-1), {
    title: "With 2025 points",
    value: "+1 place",
    detail: "Ferrari would be P1, not P2.",
  });
  assert.equal(insights(data, 2025, "rescored").at(-1).value, "Same order");
});
test("re-scoring keeps exclusions and leaves out the Indianapolis 500", () => {
  const teams = seasonStats(data, 2007, [], "rescored");
  const mclaren = teams.find((t) => t.id === 1);
  assert.deepEqual([mclaren.points, mclaren.excluded], [0, true]);
  assert.equal(mclaren.rank, Math.max(...teams.map((t) => t.rank ?? 0)));
  const p = progression(data, 2007, [1], "rescored");
  assert.ok(p.series[0].values.every((v) => v === 0));
  // Teams that only entered the Indy 500 were never in the championship.
  const indyOnly = seasonStats(data, 1958, [], "rescored").filter(
    (t) => t.officialRank == null,
  );
  assert.ok(indyOnly.length > 0);
  assert.ok(indyOnly.every((t) => t.points === null && t.rank === null));
});
test("re-scored progression adds up race by race", () => {
  const p = progression(data, 1988, [1], "rescored");
  // Brazil 1988: Prost won and Senna was disqualified.
  assert.equal(p.series[0].values[0], 25);
  assert.equal(p.series[0].values.at(-1), 576);
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
      "dashboard?scoring=2025",
      "constructors/1?scoring=new",
    ])
      assert.equal((await fetch(`${base}/api/${path}`)).status, 400, path);
    const rescored = await fetch(
      `${base}/api/standings?season=1988&scoring=rescored`,
    );
    const top = (await rescored.json()).teams[0];
    assert.deepEqual(
      [top.name, top.points, top.officialPoints],
      ["McLaren", 576, 199],
    );
    const profileResponse = await fetch(
      `${base}/api/constructors/6?scoring=rescored`,
    );
    assert.equal(profileResponse.status, 200);
    assert.equal((await profileResponse.json()).seasons[0].scoring, "rescored");
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
