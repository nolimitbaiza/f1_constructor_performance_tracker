/**
 * Statistics shared by the browser (app.js) and the REST API (server/app.js).
 *
 * Every function takes the dataset produced by scripts/build_app_data.py:
 *   { constructors, seasons, races, results, standings, meta }
 * and returns plain objects, so the same numbers appear in the UI and the API.
 *
 * Most functions take a `scoring` option:
 *   "official"  points and ranks exactly as awarded at the time
 *   "rescored"  every race scored again with today's (2025) points system,
 *               so seasons from different eras can be compared like with like
 */

/** The points systems the app can show. */
export const SCORING = ["official", "rescored"];

/**
 * Today's (2025) points tables. The re-scored points per race are calculated
 * by scripts/build_app_data.py; these are used to explain them on screen.
 */
export const POINTS_2025 = {
  grandPrix: [25, 18, 15, 12, 10, 8, 6, 4, 2, 1],
  sprint: [8, 7, 6, 5, 4, 3, 2, 1],
};

/**
 * One row per constructor that raced in `year`, sorted by championship rank.
 * Wins, podiums, races and average finish are summed from race results.
 * Official points and rank come from the standings after the final round;
 * with scoring = "rescored", points and rank use the 2025 points system
 * instead, and the official values are kept as officialPoints/officialRank.
 * Pass `ids` to keep only those constructors.
 */
export function seasonStats(data, year, ids = [], scoring = "official") {
  const races = data.races
    .filter((r) => r.season === year)
    .sort((a, b) => a.round - b.round);
  const raceIds = new Set(races.map((r) => r.id));
  // Official standings after the last race of the season.
  const finalRace = races.at(-1)?.id;
  const standings = data.standings.filter((s) => s.raceId === finalRace);
  // Add up each constructor's race-by-race results for this season.
  const totals = new Map();
  for (const r of data.results) {
    if (!raceIds.has(r.raceId)) continue;
    const t = totals.get(r.constructorId) ?? {
      id: r.constructorId,
      wins: 0,
      podiums: 0,
      races: 0,
      entries: 0,
      classified: 0,
      finishSum: 0,
      poles: 0,
      earnedPoints: 0,
      rescoredPoints: 0,
    };
    for (const key of [
      "wins",
      "podiums",
      "entries",
      "classified",
      "finishSum",
      "poles",
    ])
      t[key] += r[key];
    t.races++;
    t.earnedPoints += r.racePoints + r.sprintPoints;
    t.rescoredPoints += r.rescoredPoints ?? 0;
    totals.set(t.id, t);
  }
  // Attach official points and rank (these include penalties and exclusions).
  for (const s of standings) {
    if (!totals.has(s.constructorId)) continue;
    Object.assign(totals.get(s.constructorId), {
      officialPoints: s.points,
      officialRank: s.rank,
      excluded: s.excluded,
    });
  }
  const teams = [...totals.values()];
  if (scoring === "rescored") applyRescoring(teams);
  else
    for (const t of teams) {
      t.points = t.officialPoints;
      t.rank = t.officialRank;
    }
  // Used for "points share": a team's percentage of all points that season.
  const totalPoints = teams.reduce((sum, t) => sum + (t.points ?? 0), 0);
  return (
    teams
      .map((t) => ({
        ...t,
        ...data.constructors.find((c) => c.id === t.id),
        season: year,
        scoring,
        points: t.points ?? null,
        rank: t.rank ?? null,
        officialPoints: t.officialPoints ?? null,
        officialRank: t.officialRank ?? null,
        share:
          totalPoints && t.points != null
            ? (t.points / totalPoints) * 100
            : null,
        average: t.classified ? t.finishSum / t.classified : null,
      }))
      .filter((t) => !ids.length || ids.includes(t.id))
      // Unranked teams go last, ordered by the points they scored.
      .sort(
        (a, b) =>
          (a.rank ?? Infinity) - (b.rank ?? Infinity) ||
          b.earnedPoints - a.earnedPoints,
      )
  );
}

/**
 * Sets points and rank from the 2025-system points. Only teams in the official
 * championship are ranked (as in the official table), and teams excluded from
 * it stay excluded: they score zero and are placed last.
 * Ties are broken by wins, then podiums, then the official position.
 */
function applyRescoring(teams) {
  const ranked = teams.filter((t) => t.officialRank != null);
  for (const t of teams) {
    t.points =
      t.officialRank == null ? null : t.excluded ? 0 : t.rescoredPoints;
    t.rank = null;
  }
  ranked
    .sort(
      (a, b) =>
        a.excluded - b.excluded ||
        b.points - a.points ||
        b.wins - a.wins ||
        b.podiums - a.podiums ||
        a.officialRank - b.officialRank,
    )
    .forEach((t, i) => (t.rank = i + 1));
}

/**
 * Cumulative championship points after each round of `year`, one series per
 * constructor ID. Rounds before a team's first race are null (a gap).
 * Official points come from the standings after each race; re-scored points
 * are added up race by race (Grand Prix plus any sprint that weekend).
 */
export function progression(data, year, ids, scoring = "official") {
  const races = data.races
    .filter((r) => r.season === year)
    .sort((a, b) => a.round - b.round);
  if (scoring === "rescored") return rescoredProgression(data, races, ids);
  // Look-up table: "raceId:constructorId" -> points after that race.
  const index = new Map(
    data.standings.map((s) => [`${s.raceId}:${s.constructorId}`, s.points]),
  );
  return {
    races,
    series: ids.map((id) => ({
      id,
      name: data.constructors.find((c) => c.id === id)?.name,
      values: races.map((r) => index.get(`${r.id}:${id}`) ?? null),
    })),
  };
}

/** Re-scored version of progression(): a running total of 2025-system points. */
function rescoredProgression(data, races, ids) {
  const raceIds = new Set(races.map((r) => r.id));
  const points = new Map(
    data.results
      .filter((r) => raceIds.has(r.raceId))
      .map((r) => [`${r.raceId}:${r.constructorId}`, r.rescoredPoints ?? 0]),
  );
  // Teams excluded from the championship stay at zero, as in the official table.
  const finalRace = races.at(-1)?.id;
  const excluded = new Set(
    data.standings
      .filter((s) => s.raceId === finalRace && s.excluded)
      .map((s) => s.constructorId),
  );
  return {
    races,
    series: ids.map((id) => {
      let total = null;
      return {
        id,
        name: data.constructors.find((c) => c.id === id)?.name,
        values: races.map((r) => {
          const scored = points.get(`${r.id}:${id}`);
          if (scored !== undefined) total = (total ?? 0) + scored;
          return total == null ? null : excluded.has(id) ? 0 : total;
        }),
      };
    }),
  };
}

/**
 * A constructor's full history: one entry per season it raced, career totals,
 * title count, and its best and worst complete seasons.
 */
export function profile(data, id, scoring = "official") {
  const seasons = data.seasons
    .map((s) => ({
      ...seasonStats(data, s.year, [id], scoring)[0],
      complete: s.complete,
    }))
    .filter((s) => s.id);
  // Partial seasons are left out of best/worst and title counts.
  const complete = seasons.filter((s) => s.complete && s.rank != null);
  const byRank = [...complete].sort(
    (a, b) => a.rank - b.rank || b.share - a.share,
  );
  return {
    constructor: data.constructors.find((c) => c.id === id),
    seasons,
    best: byRank[0] ?? null,
    worst: byRank.at(-1) ?? null,
    titles: complete.filter((s) => s.rank === 1).length,
    wins: seasons.reduce((sum, s) => sum + s.wins, 0),
    podiums: seasons.reduce((sum, s) => sum + s.podiums, 0),
    races: seasons.reduce((sum, s) => sum + s.races, 0),
  };
}

/**
 * Short observations for a season: the championship leader, the team with the
 * most wins, and the biggest climb compared with the previous year. With
 * re-scored points, a fourth one shows the biggest change from the official order.
 */
export function insights(data, year, scoring = "official") {
  const teams = seasonStats(data, year, [], scoring);
  if (!teams.length) return [];
  const leader = teams[0];
  const wins = [...teams].sort((a, b) => b.wins - a.wins)[0];
  // Positions gained since last season (positive = moved up the order).
  const previous = seasonStats(data, year - 1, [], scoring);
  const changes = teams
    .map((t) => ({
      ...t,
      gain: previous.find((p) => p.id === t.id)?.rank - t.rank,
    }))
    .filter((t) => Number.isFinite(t.gain) && t.rank != null)
    .sort((a, b) => b.gain - a.gain);
  const list = [
    {
      title: "Championship lead",
      value: `${leader.name}`,
      detail: `${leader.points ?? "Unavailable"} points${teams[1]?.points != null && leader.points != null ? ` · ${+(leader.points - teams[1].points).toFixed(1)} ahead of ${teams[1].name}` : ""}.`,
    },
    {
      title: "Race-winning pace",
      value: `${wins.wins} wins`,
      detail: `${wins.name} won ${wins.wins} of ${data.seasons.find((s) => s.year === year).races} Grands Prix.`,
    },
    {
      title: "Biggest climb",
      value:
        changes[0]?.gain > 0
          ? `+${changes[0].gain} places`
          : "Holding position",
      detail:
        changes[0]?.gain > 0
          ? `${changes[0].name} versus its ${year - 1} championship position.`
          : "No returning constructor improved its championship position.",
    },
  ];
  if (scoring === "rescored") list.push(rescoringChange(teams, year));
  return list;
}

/** The team whose position changes most when the season is re-scored. */
function rescoringChange(teams, year) {
  const moved = teams
    .filter((t) => t.rank != null && t.officialRank != null && !t.excluded)
    .map((t) => ({ ...t, change: t.officialRank - t.rank }))
    .sort((a, b) => Math.abs(b.change) - Math.abs(a.change))[0];
  if (!moved?.change)
    return {
      title: "With 2025 points",
      value: "Same order",
      detail: `The ${year} order does not change with today's points system.`,
    };
  const places = Math.abs(moved.change) === 1 ? "place" : "places";
  return {
    title: "With 2025 points",
    value: `${moved.change > 0 ? "+" : "−"}${Math.abs(moved.change)} ${places}`,
    detail: `${moved.name} would be P${moved.rank}, not P${moved.officialRank}.`,
  };
}

/**
 * Converts rows to CSV text. Every cell is quoted, and values starting with
 * = + @ or - are prefixed with ' so spreadsheets do not run them as formulas.
 */
export function csv(rows, columns) {
  const cell = (value) =>
    `"${String(value ?? "")
      .replace(/^[=+@-]/, "'$&")
      .replaceAll('"', '""')}"`;
  return [
    columns.map(cell).join(","),
    ...rows.map((r) => columns.map((c) => cell(r[c])).join(",")),
  ].join("\r\n");
}
