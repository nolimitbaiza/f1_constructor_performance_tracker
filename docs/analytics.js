/**
 * Statistics shared by the browser (app.js) and the REST API (server/app.js).
 *
 * Every function takes the dataset produced by scripts/build_app_data.py:
 *   { constructors, seasons, races, results, standings, meta }
 * and returns plain objects, so the same numbers appear in the UI and the API.
 */

/**
 * One row per constructor that raced in `year`, sorted by championship rank.
 * Wins, podiums, races and average finish are summed from race results;
 * points and rank come from the official standings after the final round.
 * Pass `ids` to keep only those constructors.
 */
export function seasonStats(data, year, ids = []) {
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
    totals.set(t.id, t);
  }
  // Attach official points and rank (these include penalties and exclusions).
  for (const s of standings) {
    if (!totals.has(s.constructorId)) continue;
    Object.assign(totals.get(s.constructorId), {
      points: s.points,
      rank: s.rank,
      excluded: s.excluded,
    });
  }
  // Used for "points share": a team's percentage of all points that season.
  const totalPoints = [...totals.values()].reduce(
    (sum, t) => sum + (t.points ?? 0),
    0,
  );
  return (
    [...totals.values()]
      .map((t) => ({
        ...t,
        ...data.constructors.find((c) => c.id === t.id),
        season: year,
        points: t.points ?? null,
        rank: t.rank ?? null,
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
 * Cumulative championship points after each round of `year`, one series per
 * constructor ID. Rounds without a standing for a team are null (a gap).
 */
export function progression(data, year, ids) {
  const races = data.races
    .filter((r) => r.season === year)
    .sort((a, b) => a.round - b.round);
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

/**
 * A constructor's full history: one entry per season it raced, career totals,
 * title count, and its best and worst complete seasons.
 */
export function profile(data, id) {
  const seasons = data.seasons
    .map((s) => ({
      ...seasonStats(data, s.year, [id])[0],
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
 * Three short observations for a season: the championship leader, the team
 * with the most wins, and the biggest climb compared with the previous year.
 */
export function insights(data, year) {
  const teams = seasonStats(data, year);
  if (!teams.length) return [];
  const leader = teams[0];
  const wins = [...teams].sort((a, b) => b.wins - a.wins)[0];
  // Positions gained since last season (positive = moved up the order).
  const previous = seasonStats(data, year - 1);
  const changes = teams
    .map((t) => ({
      ...t,
      gain: previous.find((p) => p.id === t.id)?.rank - t.rank,
    }))
    .filter((t) => Number.isFinite(t.gain) && t.rank != null)
    .sort((a, b) => b.gain - a.gain);
  return [
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
