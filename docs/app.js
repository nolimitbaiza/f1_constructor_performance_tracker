/**
 * Pitwall front end.
 *
 * Loads the dataset (from the REST API, or from the bundled data.json when
 * the page is hosted statically) and renders one of four hash-routed views:
 *   #dashboard        season overview
 *   #profiles[/<id>]  constructor search and individual profiles
 *   #comparison       side-by-side comparison of 2–4 constructors
 *   #insights         rankings, title counts and calculated observations
 *
 * All statistics come from analytics.js, which the Express API also uses.
 */
import {
  seasonStats,
  progression,
  profile,
  insights,
  csv,
  SCORING,
  POINTS_2025,
} from "./analytics.js";

/* ---------- Small helpers ---------- */

/** Shorthand for document.getElementById. */
const $ = (id) => document.getElementById(id);

/** Escapes text before it is inserted into HTML, to prevent injection. */
const escape = (value) =>
  String(value ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );

/** Formats a number for display (e.g. 2659 → "2,659"); missing values show "—". */
const number = (value) =>
  value == null
    ? "—"
    : Number(value).toLocaleString("en-US", { maximumFractionDigits: 1 });

/** Fixed colours for well-known teams, keyed by constructor reference. */
const colors = {
  mclaren: "#ff963d",
  ferrari: "#ff4b5b",
  mercedes: "#40d6bf",
  red_bull: "#6692ff",
  williams: "#58baff",
  aston_martin: "#69b397",
  alpine: "#e68ac7",
  sauber: "#a5d85b",
  rb: "#9f9aff",
  haas: "#ccd2dc",
  renault: "#ffe16b",
  toro_rosso: "#8995e3",
  racing_point: "#ee9dcb",
};

/* ---------- Application state ---------- */

let data; // full dataset: constructors, seasons, races, results, standings
let year; // currently selected season
let page = "dashboard"; // current route
let selected = null; // constructor IDs chosen on the comparison page
let charts = []; // Chart.js instances, destroyed before each re-render
let exportedRows = []; // rows written by the "Export results" CSV button
let scoring = loadScoring(); // "official" or "rescored" (2025 points system)

/* ---------- Points system ---------- */

/** True when the re-scored (2025 points system) view is selected. */
const rescored = () => scoring === "rescored";

/** Season statistics for the selected points system. */
const stats = (y, ids = []) => seasonStats(data, y, ids, scoring);

/** Reads the last chosen points system; storage can be blocked, so default to official. */
function loadScoring() {
  try {
    const saved = localStorage.getItem("pitwall-scoring");
    return SCORING.includes(saved) ? saved : "official";
  } catch {
    return "official";
  }
}

/** Remembers the chosen points system for the next visit (if storage is allowed). */
function saveScoring() {
  try {
    localStorage.setItem("pitwall-scoring", scoring);
  } catch {
    // Private browsing or blocked storage: the choice just is not remembered.
  }
}

/**
 * Returns a constructor's colour. Teams without a fixed colour get a stable
 * colour generated from their ID, so they look the same on every page.
 */
const color = (id) =>
  colors[data.constructors.find((c) => c.id === id)?.ref] ??
  `hsl(${(id * 137) % 360} 55% 65%)`;

/* ---------- Reusable HTML builders ---------- */

/** Team name with a coloured stripe, linking to the team's profile. */
const teamName = (t) => `
  <a class="team-link" href="#profiles/${t.id}">
    <span class="team-stripe" style="--team:${color(t.id)}"></span>${escape(t.name)}
  </a>`;

/** A headline number card, e.g. "Grand Prix wins: 24". */
const metric = (label, value, detail, icon) => `
  <div class="metric">
    <div class="metric-label">
      ${label}<span class="metric-icon" aria-hidden="true">${icon}</span>
    </div>
    <div class="metric-value">${value}</div>
    <div class="metric-detail">${detail}</div>
  </div>`;

/** Heading shared by text panels and chart panels. */
const panelHead = (title, subtitle, actions = "") => `
  <div class="panel-head">
    <div>
      <h2>${title}</h2>
      <p class="panel-subtitle">${subtitle}</p>
    </div>
    ${actions}
  </div>`;

/** A content panel with a title, subtitle and arbitrary body HTML. */
const panel = (title, subtitle, body, extra = "") => `
  <section class="panel ${extra}">
    ${panelHead(title, subtitle)}
    ${body}
  </section>`;

/**
 * A panel holding an empty <canvas>. The chart itself is drawn afterwards by
 * makeChart(); the PNG button and legend are wired up in render().
 */
const chartPanel = (id, title, subtitle, extra = "") => `
  <section class="panel ${extra}">
    ${panelHead(
      title,
      subtitle,
      `<button class="small-button" data-chart="${id}" aria-label="Download ${title} as PNG">PNG</button>`,
    )}
    <div class="chart-wrap">
      <canvas id="${id}" role="img" aria-label="${title}"></canvas>
    </div>
    <div class="chart-legend" id="${id}-legend"></div>
  </section>`;

/** Wraps table markup so wide tables scroll horizontally on small screens. */
const scrollTable = (head, rows, className = "standings") => `
  <div class="table-scroll">
    <table class="${className}">
      <thead><tr>${head.map((h) => `<th scope="col">${h}</th>`).join("")}</tr></thead>
      <tbody>${rows.join("")}</tbody>
    </table>
  </div>`;

/** One standings row. Compact rows show a points bar instead of extra columns. */
function standingsRow(t, compact, maxPoints) {
  const rank = t.excluded
    ? "EX"
    : t.rank == null
      ? "—"
      : String(t.rank).padStart(2, "0");
  if (compact) {
    const width = Math.max(0, ((t.points ?? 0) / maxPoints) * 100);
    return `
      <tr>
        <td class="rank">${rank}</td>
        <td>${teamName(t)}</td>
        <td class="points">
          ${number(t.points)}
          <div class="bar-track">
            <div class="bar-fill" style="width:${width}%;--team:${color(t.id)}"></div>
          </div>
        </td>
      </tr>`;
  }
  return `
    <tr>
      <td class="rank">${rank}</td>
      <td>${teamName(t)}</td>
      <td class="points">${number(t.points)}</td>
      ${rescored() ? officialCell(t) : ""}
      <td>${t.wins}</td>
      <td>${t.podiums}</td>
      <td>${t.races}</td>
      <td>${number(t.average)}</td>
    </tr>`;
}

/**
 * Table cell with the official position, shown next to re-scored points.
 * ▲ means the team ranks higher with 2025 points, ▼ lower.
 */
function officialCell(t) {
  if (t.officialRank == null) return "<td>—</td>";
  if (t.excluded) return "<td>EX</td>";
  const change = t.officialRank - t.rank;
  const places = Math.abs(change) === 1 ? "place" : "places";
  const badge = change
    ? `<span class="change ${change > 0 ? "up" : "down"}" title="${Math.abs(change)} ${places} ${change > 0 ? "higher" : "lower"} with 2025 points">${change > 0 ? "▲" : "▼"}${Math.abs(change)}</span>`
    : "";
  return `<td class="official">P${t.officialRank} ${badge}</td>`;
}

/** Constructor standings table (compact = top-five summary version). */
function table(rows, compact = false) {
  const maxPoints = Math.max(1, ...rows.map((t) => t.points ?? 0));
  const head = compact
    ? ["POS", "CONSTRUCTOR", "POINTS"]
    : [
        "POS",
        "CONSTRUCTOR",
        "POINTS",
        ...(rescored() ? ["OFFICIAL"] : []),
        "WINS",
        "PODIUMS",
        "RACES",
        "AVG FINISH",
      ];
  return scrollTable(
    head,
    rows.map((t) => standingsRow(t, compact, maxPoints)),
    `standings ${compact ? "compact" : ""}`,
  );
}

/** Numbered list of automatically calculated observations for the season. */
function observationList() {
  const rows = insights(data, year, scoring).map(
    (item, n) => `
      <article class="insight-row">
        <div class="insight-number">${String(n + 1).padStart(2, "0")}</div>
        <div>
          <h3>${item.title} <span class="muted">/</span> <strong>${escape(item.value)}</strong></h3>
          <p>${escape(item.detail)}</p>
        </div>
      </article>`,
  );
  return `<div class="insight-list">${rows.join("")}</div>`;
}

/* ---------- Charts ---------- */

/**
 * Creates a Chart.js chart on the canvas with the given ID, using the shared
 * dark theme, and builds clickable legend buttons that toggle each series.
 */
function makeChart(id, type, labels, datasets, options = {}) {
  const chart = new Chart($(id), {
    type,
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { display: false }, // replaced by the HTML legend below
        tooltip: {
          backgroundColor: "#292f37",
          padding: 12,
          titleColor: "#fff",
          bodyColor: "#d5dbe3",
        },
      },
      scales: {
        x: {
          grid: { display: false },
          ticks: {
            color: "#919aa8",
            font: { size: 10 },
            maxRotation: 0,
            autoSkip: true,
            maxTicksLimit: 8,
          },
          border: { display: false },
        },
        y: {
          beginAtZero: true,
          grid: { color: "#2d3239", drawTicks: false },
          ticks: { color: "#919aa8", padding: 10, font: { size: 10 } },
          border: { display: false },
        },
      },
      ...options,
    },
  });
  charts.push(chart);

  // Accessible legend: one toggle button per data series.
  const legend = $(`${id}-legend`);
  if (legend)
    legend.innerHTML = datasets
      .map((d, i) => {
        const swatch = Array.isArray(d.borderColor) ? "#aaa" : d.borderColor;
        return `
          <button class="small-button" data-toggle-chart="${id}" data-series="${i}" aria-pressed="true">
            <span class="swatch" style="--team:${swatch}"></span>${escape(d.label)}
          </button>`;
      })
      .join("");
}

/** Line chart with one series per constructor, in each team's colour. */
function line(id, labels, series) {
  makeChart(
    id,
    "line",
    labels,
    series.map((s) => ({
      label: s.name,
      data: s.values,
      borderColor: color(s.id),
      backgroundColor: color(s.id),
      borderWidth: 2,
      pointRadius: labels.length > 30 ? 0 : 2, // hide dots on long histories
      pointHoverRadius: 5,
      tension: 0.18,
      spanGaps: false,
    })),
  );
}

/** Cumulative championship points after each round of the selected season. */
function pointsChart(id, ids) {
  const p = progression(data, year, ids, scoring);
  line(
    id,
    p.races.map((r) => `R${r.round} · ${r.name.replace(" Grand Prix", "")}`),
    p.series,
  );
}

/** Bar chart with a single dataset (one bar per label). */
function barChart(id, labels, label, values, colours, borderColor) {
  makeChart(id, "bar", labels, [
    {
      label,
      data: values,
      backgroundColor: colours,
      borderColor,
      borderRadius: 3,
    },
  ]);
}

/* ---------- View: Overview (#dashboard) ---------- */

function dashboard() {
  const all = stats(year);
  const id = $("constructor").value;
  const teams = id === "all" ? all : all.filter((t) => t.id === Number(id));
  const sum = (key) => teams.reduce((s, t) => s + (t[key] ?? 0), 0);
  const season = data.seasons.find((s) => s.year === year);
  exportedRows = teams;

  const metrics = [
    metric(
      rescored() ? "Points · 2025 system" : "Championship points",
      number(sum("points")),
      id === "all"
        ? `Across <em>${all.length} constructors</em>`
        : rescored()
          ? "Re-scored total"
          : "Official championship total",
      "◈",
    ),
    metric(
      "Grand Prix wins",
      number(sum("wins")),
      "Race victories · sprints excluded",
      "⚑",
    ),
    metric(
      "Podium finishes",
      number(sum("podiums")),
      "Top-three Grand Prix finishes",
      "▥",
    ),
    metric(
      "Grands Prix",
      number(id === "all" ? season.races : (teams[0]?.races ?? 0)),
      `Of ${season.scheduled} scheduled rounds`,
      "▦",
    ),
  ];
  const topFive = `
    ${table(all.slice(0, 5), true)}
    <div class="table-bottom">
      <span>Explore the teams behind the numbers</span>
      <a href="#profiles">View teams</a>
    </div>`;

  $("view").innerHTML = `
    <div class="metrics">${metrics.join("")}</div>
    <div class="grid-main">
      ${chartPanel("progress", "Championship progression", `Cumulative ${rescored() ? "points (2025 system)" : "official points"} after each Grand Prix`)}
      ${panel("Constructor standings", `${year} season · top five constructors`, topFive)}
    </div>
    <div class="bottom-grid">
      ${chartPanel("wins", "Who owns the podium?", "Grand Prix wins and podium finishes")}
      ${panel("From the pitwall", "The story behind this season’s numbers", observationList())}
    </div>
    ${panel("Season breakdown", "Average finish includes classified Grand Prix finishes only.", table(teams))}`;

  pointsChart(
    "progress",
    teams.slice(0, 5).map((t) => t.id),
  );
  const leaders = teams.slice(0, 6);
  makeChart(
    "wins",
    "bar",
    leaders.map((t) => t.name),
    [
      {
        label: "Wins",
        data: leaders.map((t) => t.wins),
        backgroundColor: "#ff765e",
        borderColor: "#ff765e",
        borderRadius: 3,
      },
      {
        label: "Podiums",
        data: leaders.map((t) => t.podiums),
        backgroundColor: "#66768e",
        borderColor: "#66768e",
        borderRadius: 3,
      },
    ],
  );
}

/* ---------- View: Constructors (#profiles and #profiles/<id>) ---------- */

/** Card linking to a constructor profile, used in the team grid. */
function teamCard(t, searching) {
  const summary = searching
    ? "Explore its championship history"
    : `${year} championship · P${t.rank ?? "—"}`;
  const stats = searching
    ? ["Historical profile", "Season archive"]
    : [`${number(t.points)} points`, `${t.wins} wins`];
  return `
    <a class="team-card" href="#profiles/${t.id}" style="--team:${color(t.id)}">
      <div class="eyebrow">${escape(t.nationality)} CONSTRUCTOR</div>
      <h2>${escape(t.name)}</h2>
      <p>${summary}</p>
      <div class="team-stats"><span>${stats[0]}</span><span>${stats[1]}</span></div>
    </a>`;
}

/**
 * Without an ID: a searchable grid of constructors (this season's teams by
 * default, any team in the archive while searching).
 * With an ID: that constructor's historical profile.
 */
function profiles(id) {
  if (!id) {
    const teams = stats(year);
    exportedRows = teams;
    $("view").innerHTML = `
      <form id="team-search-form" role="search">
        <label class="muted" for="team-search">Find a constructor across the archive</label>
        <input class="search" id="team-search" type="search" placeholder="Search Ferrari, McLaren, Lotus…" />
      </form>
      <div class="team-grid" id="team-grid"></div>`;

    // Re-draw the grid on every keystroke.
    const draw = (query) => {
      const list = query
        ? data.constructors.filter(
            (t) =>
              t.name.toLowerCase().includes(query.toLowerCase()) &&
              data.results.some((r) => r.constructorId === t.id),
          )
        : teams;
      $("team-grid").innerHTML = list.length
        ? list.map((t) => teamCard(t, Boolean(query))).join("")
        : '<p class="empty">No constructors match your search.</p>';
    };
    draw("");
    $("team-search").addEventListener("input", (event) =>
      draw(event.target.value),
    );
    // Results update live, so pressing Enter must not reload the page.
    $("team-search-form").addEventListener("submit", (event) =>
      event.preventDefault(),
    );
    return;
  }

  const p = profile(data, id, scoring);
  if (!p.constructor || !p.seasons.length) {
    $("view").innerHTML =
      '<div class="empty">Constructor not found. <a href="#profiles">Browse constructors</a></div>';
    exportedRows = [];
    return;
  }
  const t = p.constructor;
  $("page-title").innerHTML = `${escape(t.name)}<span>.</span>`;
  $("page-description").textContent =
    `${t.nationality} constructor · ${p.seasons[0].season}–${p.seasons.at(-1).season} in this archive`;
  exportedRows = p.seasons;

  const best = (s) =>
    s
      ? `<strong>${s.season}</strong><span>P${s.rank} · ${number(s.points)} points</span>`
      : "<strong>—</strong>";
  const seasonRows = [...p.seasons].reverse().map(
    (s) => `
      <tr>
        <td>${s.season}${s.complete ? "" : " *"}</td>
        <td>${s.excluded ? "EX" : `P${s.rank ?? "—"}`}</td>
        ${rescored() ? officialCell(s) : ""}
        <td>${number(s.points)}</td>
        <td>${number(s.share)}%</td>
        <td>${s.wins}</td>
        <td>${s.podiums}</td>
        <td>${number(s.average)}</td>
      </tr>`,
  );
  const metrics = [
    metric(
      rescored() ? "Titles · 2025 system" : "Constructor titles",
      p.titles,
      rescored()
        ? "If every season used today's points"
        : "Completed seasons since 1958",
      "◈",
    ),
    metric(
      "Grand Prix wins",
      number(p.wins),
      "Across the available archive",
      "⚑",
    ),
    metric("Podiums", number(p.podiums), "Grand Prix top-three finishes", "▥"),
    metric(
      "Race appearances",
      number(p.races),
      "Unique constructor race weekends",
      "▦",
    ),
  ];

  $("view").innerHTML = `
    <section class="profile-banner" style="--team:${color(id)}">
      <div>
        <div class="eyebrow">CONSTRUCTOR DOSSIER</div>
        <h2>${escape(t.name)}</h2>
        <p>${p.seasons.length} seasons of racing history.<br>Constructor identities are kept separate across name changes.</p>
      </div>
      <div class="profile-bests">
        <div><span>BEST COMPLETE SEASON</span>${best(p.best)}</div>
        <div><span>WORST COMPLETE SEASON</span>${best(p.worst)}</div>
      </div>
    </section>
    <div class="metrics">${metrics.join("")}</div>
    <div class="bottom-grid">
      ${chartPanel("history", "Through the seasons", "Share of all championship points · %")}
      ${chartPanel("profile-progress", `${year} points progression`, rescored() ? "Points by round · 2025 system" : "Official championship points by round")}
    </div>
    ${panel(
      "Season-by-season record",
      "Best/worst ranked by championship position, then points share. Partial seasons excluded.",
      scrollTable(
        [
          "SEASON",
          "POSITION",
          ...(rescored() ? ["OFFICIAL"] : []),
          "POINTS",
          "SHARE",
          "WINS",
          "PODIUMS",
          "AVG FINISH",
        ],
        seasonRows,
      ),
    )}
    <div class="callout">
      * Partial season. ${
        rescored()
          ? "Every season is re-scored with today's points, so the numbers can be compared across eras."
          : "Raw points are not directly comparable across scoring eras. Points share provides context; switch Points to the 2025 system to compare eras like with like."
      }
    </div>`;

  line(
    "history",
    p.seasons.map((s) => s.season),
    [{ id, name: t.name, values: p.seasons.map((s) => s.share) }],
  );
  pointsChart("profile-progress", [id]);
  if (!p.seasons.some((s) => s.season === year))
    $("profile-progress-legend").innerHTML =
      "<span>No race entries in the selected season.</span>";
}

/* ---------- View: Compare teams (#comparison) ---------- */

/** Checkbox for one constructor. At most four can be selected at once. */
function teamOption(t) {
  const checked = selected.includes(t.id);
  const disabled = selected.length >= 4 && !checked;
  return `
    <label class="team-option" style="--team:${color(t.id)}">
      <input type="checkbox" name="constructor" value="${t.id}" ${checked ? "checked" : ""} ${disabled ? "disabled" : ""}>
      ${escape(t.name)}
    </label>`;
}

function comparison() {
  const teams = stats(year);
  const validIds = new Set(teams.map((t) => t.id));
  // Default to the top three; drop teams that did not race in a newly chosen season.
  selected =
    selected === null
      ? teams.slice(0, 3).map((t) => t.id)
      : selected.filter((id) => validIds.has(id));
  const chosen = teams.filter((t) => selected.includes(t.id));
  exportedRows = chosen;

  $("view").innerHTML = `
    <form class="panel selection" id="compare-form">
      <fieldset>
        <legend>Choose 2–4 constructors to compare. All teams shown raced in ${year}.</legend>
        <div class="team-options">${teams.map(teamOption).join("")}</div>
      </fieldset>
    </form>
    <div id="compare-content"></div>`;

  // Re-render whenever a checkbox changes; the form is never submitted.
  $("compare-form").addEventListener("change", () => {
    selected = [...$("compare-form").querySelectorAll("input:checked")].map(
      (input) => Number(input.value),
    );
    render();
  });
  $("compare-form").addEventListener("submit", (event) =>
    event.preventDefault(),
  );

  if (selected.length < 2) {
    $("compare-content").innerHTML =
      '<div class="empty">Select at least two constructors to see their comparison.</div>';
    return;
  }

  $("compare-content").innerHTML = `
    <div class="bottom-grid">
      ${chartPanel("comparison-points", "Head to head", rescored() ? "Points by round · 2025 system" : "Official championship points by round")}
      ${chartPanel("comparison-history", "The longer rivalry", "Share of championship points across seasons · %")}
    </div>
    ${panel("Side-by-side results", `${year} season · average classified Grand Prix finish`, table(chosen))}
    <div class="bottom-grid section-gap" style="margin-top:24px">
      ${chartPanel("comparison-wins", "Race wins", "Grand Prix victories in the selected season")}
      ${chartPanel("comparison-podiums", "Podium finishes", "Top-three Grand Prix finishes in the selected season")}
    </div>`;

  pointsChart("comparison-points", selected);

  // Points share for every season in which at least one chosen team raced.
  const histories = selected.map((id) => profile(data, id, scoring));
  const years = data.seasons
    .map((s) => s.year)
    .filter((y) =>
      histories.some((p) => p.seasons.some((s) => s.season === y)),
    );
  line(
    "comparison-history",
    years,
    histories.map((p) => ({
      id: p.constructor.id,
      name: p.constructor.name,
      values: years.map(
        (y) => p.seasons.find((s) => s.season === y)?.share ?? null,
      ),
    })),
  );

  for (const field of ["wins", "podiums"])
    barChart(
      `comparison-${field}`,
      chosen.map((t) => t.name),
      field === "wins" ? "Wins" : "Podiums",
      chosen.map((t) => t[field]),
      chosen.map((t) => color(t.id)),
      "#c6cbd3",
    );
}

/* ---------- View: Insights & analytics (#insights) ---------- */

function analytics() {
  const teams = stats(year);
  exportedRows = teams;
  const top = teams.slice(0, 5);

  // Count constructors' titles across all completed seasons.
  const titles = new Map();
  for (const season of data.seasons.filter((s) => s.complete)) {
    const winner = stats(season.year).find((t) => t.rank === 1);
    if (winner) titles.set(winner.id, (titles.get(winner.id) ?? 0) + 1);
  }
  const champions = [...titles].sort((a, b) => b[1] - a[1]).slice(0, 8);

  $("view").innerHTML = `
    <div class="bottom-grid">
      ${panel("The season in focus", "Automatically calculated from the selected season", observationList())}
      ${chartPanel("dominance", "An era of their own", `Constructor titles · completed seasons since 1958${rescored() ? " · 2025 system" : ""}`)}
    </div>
    <div class="bottom-grid">
      ${chartPanel("share", "Share of the championship", `${year} · percentage of ${rescored() ? "points (2025 system)" : "official points"}`)}
      ${chartPanel("trends", "Performance across seasons", "Selected season’s top five teams · points share %")}
    </div>
    ${panel(
      "Constructor rankings",
      rescored()
        ? "Order with today's points, next to the official position."
        : "Official championship order, with race-level performance alongside it.",
      table(teams),
    )}
    <div class="callout">
      ${
        rescored()
          ? "Points and ranks are re-scored with the 2025 system. Teams excluded from a championship stay excluded. Ties are split by wins, then podiums."
          : "Championship points and ranks use official standings, including adjustments."
      }
      Wins and podiums are Grand Prix results; sprint points contribute to championship points.
      Average finish excludes unclassified results. The latest partial season is not counted as a title.
    </div>`;

  barChart(
    "dominance",
    champions.map(([id]) => data.constructors.find((t) => t.id === id).name),
    "Titles",
    champions.map(([, count]) => count),
    champions.map(([id]) => color(id)),
    "#aeb6c5",
  );
  barChart(
    "share",
    teams.map((t) => t.name),
    "Points share %",
    teams.map((t) => t.share),
    teams.map((t) => color(t.id)),
    "#aeb6c5",
  );

  // Points share of this season's top five over the previous 15 seasons.
  const years = data.seasons
    .filter((s) => s.year <= year && s.year >= year - 14)
    .map((s) => s.year);
  line(
    "trends",
    years,
    top.map((t) => ({
      id: t.id,
      name: t.name,
      values: years.map((y) => stats(y, [t.id])[0]?.share ?? null),
    })),
  );
}

/* ---------- Routing and rendering ---------- */

/** Eyebrow, heading, description and breadcrumb text for each route. */
const titles = {
  dashboard: [
    "THE BIG PICTURE",
    "Constructor overview",
    "Follow the fight for the constructors’ championship.",
    "Overview",
  ],
  profiles: [
    "BEHIND THE BADGE",
    "Constructor profiles",
    "Explore the teams that shaped Formula 1.",
    "Constructors",
  ],
  comparison: [
    "THE RIVALRIES",
    "Compare constructors",
    "Two teams. Different stories. One grid.",
    "Compare teams",
  ],
  insights: [
    "READ BETWEEN THE LAPS",
    "Insights & analytics",
    "Find the patterns behind the championship.",
    "Insights & analytics",
  ],
};

/** Reads the URL hash, updates the page chrome and draws the matching view. */
function render() {
  charts.forEach((c) => c.destroy());
  charts = [];
  const [route, rawId] = location.hash.slice(1).split("/");
  page = Object.hasOwn(titles, route) ? route : "dashboard";

  // Page heading, breadcrumb and active navigation link.
  const [eyebrow, heading, description, crumb] = titles[page];
  $("eyebrow").textContent = eyebrow;
  $("page-title").innerHTML = `${heading}<span>.</span>`;
  $("page-description").textContent = description;
  $("breadcrumb").textContent = crumb;
  document.querySelectorAll("nav a").forEach((a) => {
    a.classList.toggle("active", a.dataset.page === page);
    if (a.dataset.page === page) a.setAttribute("aria-current", "page");
    else a.removeAttribute("aria-current");
  });

  // The constructor filter only applies to the overview.
  $("constructor-filter").hidden = page !== "dashboard";
  // Points system: keep the radio buttons and the explanation in sync.
  document.querySelector(`input[name="scoring"][value="${scoring}"]`).checked =
    true;
  $("scoring-note").hidden = !rescored();
  const season = data.seasons.find((s) => s.year === year);
  $("coverage").innerHTML = `
    <span class="tag ${season.complete ? "" : "partial"}">${season.complete ? "COMPLETE SEASON" : "PARTIAL SEASON"}</span>
    ${season.races} / ${season.scheduled} ROUNDS`;

  if (page === "dashboard") dashboard();
  if (page === "profiles") profiles(rawId ? Number(rawId) : null);
  if (page === "comparison") comparison();
  if (page === "insights") analytics();

  // Buttons are recreated on every render, so attach their handlers here.
  document
    .querySelectorAll("[data-chart]")
    .forEach((button) =>
      button.addEventListener("click", () => exportChart(button)),
    );
  document
    .querySelectorAll("[data-toggle-chart]")
    .forEach((button) =>
      button.addEventListener("click", () => toggleSeries(button)),
    );
}

/* ---------- Exports and chart controls ---------- */

/**
 * Downloads a chart as a PNG with a title, the selected season and a source
 * line. The legend is shown temporarily so the image is self-explanatory.
 */
function exportChart(button) {
  const chart = charts.find((c) => c.canvas.id === button.dataset.chart);
  chart.options.plugins.legend.display = true;
  chart.options.plugins.legend.labels = { color: "#e2e5e9" };
  chart.update("none");

  // Draw the chart onto a larger canvas with room for a header and footer.
  const canvas = document.createElement("canvas");
  canvas.width = chart.canvas.width;
  canvas.height = chart.canvas.height + 70;
  const ctx = canvas.getContext("2d");
  ctx.fillStyle = "#191c20";
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = "#f4f5f7";
  ctx.font = "16px Arial";
  const title = button.closest(".panel").querySelector("h2").textContent;
  ctx.fillText(
    `${title} · ${year}${rescored() ? " · 2025 points system" : ""}`,
    20,
    25,
  );
  ctx.drawImage(chart.canvas, 0, 40);
  ctx.fillStyle = "#a0a6b1";
  ctx.font = "10px Arial";
  ctx.fillText(
    `PITWALL · Archive through ${data.meta.through}`,
    20,
    canvas.height - 10,
  );
  download(
    canvas.toDataURL("image/png"),
    `pitwall-${button.dataset.chart}-${year}.png`,
  );

  chart.options.plugins.legend.display = false;
  chart.update("none");
}

/** Shows or hides one data series when its legend button is clicked. */
function toggleSeries(button) {
  const chart = charts.find((c) => c.canvas.id === button.dataset.toggleChart);
  const index = Number(button.dataset.series);
  const visible = !chart.isDatasetVisible(index);
  chart.setDatasetVisibility(index, visible);
  button.setAttribute("aria-pressed", String(visible));
  button.style.opacity = visible ? "1" : ".45";
  chart.update();
}

/** Downloads the rows shown on the current page as a CSV file. */
function exportCsv() {
  const columns = [
    "name",
    "season",
    "rank",
    "points",
    "share",
    "wins",
    "podiums",
    "races",
    "average",
    "excluded",
    "complete",
    "scoring",
    "officialRank",
    "officialPoints",
  ];
  const blob = new Blob([csv(exportedRows, columns)], {
    type: "text/csv;charset=utf-8",
  });
  const url = URL.createObjectURL(blob);
  download(url, `pitwall-${page}-${year}.csv`);
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

/** Starts a file download from a URL (data: or blob:). */
function download(url, filename) {
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
}

/* ---------- Filters ---------- */

/** Fills the constructor dropdown with the teams of the selected season. */
function updateConstructors() {
  const old = $("constructor").value;
  const teams = stats(year);
  $("constructor").innerHTML =
    '<option value="all">All constructors</option>' +
    teams
      .map((t) => `<option value="${t.id}">${escape(t.name)}</option>`)
      .join("");
  // Keep the previous choice if that team also raced in the new season.
  $("constructor").value = teams.some((t) => String(t.id) === old)
    ? old
    : "all";
}

/** Fills the season dropdown, newest first; partial seasons are labelled. */
function fillSeasons() {
  $("season").innerHTML = [...data.seasons]
    .reverse()
    .map(
      (s) =>
        `<option value="${s.year}">${s.year}${s.complete ? "" : " · partial"}</option>`,
    )
    .join("");
  $("season").value = year;
}

/* ---------- Start-up ---------- */

/** Loads the dataset, sets up the filters and event handlers, then renders. */
async function init() {
  try {
    if (window.PITWALL_DATA) {
      // Standalone Pitwall.html: the dataset is embedded in the page.
      data = window.PITWALL_DATA;
    } else {
      // Use the API when the Node server is running, otherwise the static snapshot.
      let response = await fetch("api/data");
      if (response.status === 404) response = await fetch("data.json");
      if (!response.ok) throw new Error("Unable to retrieve the dataset");
      data = await response.json();
    }
    if (!data.seasons?.length) throw new Error("No seasons have been imported");
    if (!window.Chart) throw new Error("The chart library could not load");

    // Default to the latest complete season.
    year =
      [...data.seasons].reverse().find((s) => s.complete)?.year ??
      data.seasons.at(-1).year;
    fillSeasons();
    $("data-label").textContent = `ARCHIVE THROUGH ${data.meta.through}`;
    $("archive-range").textContent =
      `${data.seasons[0].year} — ${data.seasons.at(-1).year}`;
    const latest = data.seasons.at(-1);
    $("footer-coverage").textContent =
      `Results through ${data.meta.through} · ${latest.complete ? `${latest.year} season complete` : `${latest.year}: ${latest.races} of ${latest.scheduled} rounds`} · Updated after each race weekend`;
    updateConstructors();

    // Filter form: changes apply immediately, so it is never submitted.
    $("filter-form").addEventListener("submit", (event) =>
      event.preventDefault(),
    );
    $("season").addEventListener("change", () => {
      year = Number($("season").value);
      selected = null;
      updateConstructors();
      render();
    });
    $("constructor").addEventListener("change", render);
    // Points system: official or re-scored with today's (2025) points.
    // Each points list stays on one line (class "nowrap") so numbers never split.
    $("scoring-note").innerHTML =
      `Re-scored with today's points: <strong class="nowrap">${POINTS_2025.grandPrix.join("-")}</strong> ` +
      `for the top ten in every Grand Prix and <strong class="nowrap">${POINTS_2025.sprint.join("-")}</strong> ` +
      `in sprints. Every race counts, both cars score, and there is no fastest-lap point. ` +
      `▲ and ▼ show the change from the official position.`;
    for (const input of document.querySelectorAll('input[name="scoring"]'))
      input.addEventListener("change", () => {
        scoring = input.value;
        saveScoring();
        updateConstructors();
        render();
      });
    $("export").addEventListener("click", exportCsv);
    window.addEventListener("hashchange", render);

    $("status").hidden = true;
    $("app").hidden = false;
    render();
  } catch (error) {
    // Show a readable message instead of an empty page.
    $("app").hidden = true;
    $("status").hidden = false;
    $("status").className = "status error";
    $("status").textContent =
      `${error.message}. Reload to retry, or check the local server.`;
  }
}

init();
