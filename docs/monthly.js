/**
 * Pitwall monthly explorer: constructor points scored in each calendar month since 1958.
 *
 * Reads f1_data.csv (written by the monthly pipeline in src/tracker/) and shows three views,
 * switched by the address:
 *   #analysis  one season: points by team for the year or one month, as bars or cumulative lines
 *   #compare   two to four analysis panels side by side
 *   #history   total points per team over any period
 * Every table can be downloaded as CSV and every chart as PNG.
 */
import { csv } from "./analytics.js";
import { theme, token, setupTheme, setupHeader } from "./theme.js";

const $ = (id) => document.getElementById(id);
const MONTHS = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
];

/** Escapes text before it is inserted into HTML. */
const escape = (value) =>
  String(value ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
const number = (value) =>
  value.toLocaleString("en-US", { maximumFractionDigits: 1 });

/** Line colours for the teams in a chart, in order: one set per theme so they stay readable. */
const PALETTE = {
  light: [
    "#2563eb",
    "#dc2626",
    "#ea580c",
    "#0d9488",
    "#7c3aed",
    "#db2777",
    "#65a30d",
    "#0284c7",
    "#ca8a04",
    "#64748b",
    "#15803d",
    "#be185d",
    "#4f46e5",
    "#a16207",
    "#0891b2",
    "#9333ea",
  ],
  dark: [
    "#6692ff",
    "#ff4b5b",
    "#ff963d",
    "#40d6bf",
    "#9f9aff",
    "#e68ac7",
    "#a5d85b",
    "#58baff",
    "#ffe16b",
    "#ccd2dc",
    "#69b397",
    "#ee9dcb",
    "#8995e3",
    "#e8c27a",
    "#22d3ee",
    "#c084fc",
  ],
};

let rows = []; // { season, date, month (1-12), team, points }
let panels = []; // the analysis/compare panels on screen
let historyChart = null;
let historyRows = [];

/* ---------- Data ---------- */

/** Parses f1_data.csv: Season,Date,ConstructorName,Points (no quoted fields). */
function parse(text) {
  const [header, ...lines] = text.trim().split(/\r?\n/);
  if (header.trim() !== "Season,Date,ConstructorName,Points")
    throw new Error("Unexpected columns in f1_data.csv");
  return lines.map((line) => {
    const [season, date, team, points] = line.split(",");
    return {
      season: Number(season),
      date,
      month: Number(date.slice(5, 7)),
      team,
      points: Number(points) || 0,
    };
  });
}

/** Total points per team for the given rows, highest first. */
function standings(list) {
  const totals = new Map();
  for (const r of list)
    totals.set(r.team, (totals.get(r.team) ?? 0) + r.points);
  return [...totals]
    .map(([team, points]) => ({ team, points }))
    .sort((a, b) => b.points - a.points);
}

const seasons = () =>
  [...new Set(rows.map((r) => r.season))].sort((a, b) => b - a);
const teamsIn = (list) => [...new Set(list.map((r) => r.team))].sort();
const options = (values, labels = values) =>
  values
    .map((v, i) => `<option value="${escape(v)}">${escape(labels[i])}</option>`)
    .join("");

/* ---------- Charts and downloads ---------- */

/** The shared chart look: theme colours, no grid lines on x, tooltips in the site's style. */
function chartOptions(extra = {}) {
  return {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: token("--ink"),
        titleColor: token("--bg"),
        bodyColor: token("--bg"),
        padding: 10,
        cornerRadius: 10,
      },
    },
    scales: {
      x: {
        grid: { display: false },
        border: { display: false },
        ticks: {
          color: token("--muted"),
          font: { size: 11 },
          maxRotation: 0,
          autoSkip: true,
        },
      },
      y: {
        beginAtZero: true,
        border: { display: false },
        grid: { color: token("--line"), drawTicks: false },
        ticks: { color: token("--muted"), padding: 8, font: { size: 11 } },
      },
    },
    ...extra,
  };
}

/** Starts a file download from a URL (data: or blob:). */
function download(url, filename) {
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
}

/** Downloads a table's rows as CSV (formula-safe, see analytics.js). */
function downloadCsv(list, filename) {
  const blob = new Blob(
    [
      csv(
        list.map((r, i) => ({
          rank: i + 1,
          constructor: r.team,
          points: r.points,
        })),
        ["rank", "constructor", "points"],
      ),
    ],
    { type: "text/csv;charset=utf-8" },
  );
  const url = URL.createObjectURL(blob);
  download(url, filename);
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

/** Downloads a chart as PNG on the page's background, with its title and a source line. */
function downloadPng(chart, title, filename) {
  const canvas = document.createElement("canvas");
  canvas.width = chart.canvas.width;
  canvas.height = chart.canvas.height + 70;
  const ctx = canvas.getContext("2d");
  ctx.fillStyle = token("--surface");
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = token("--ink");
  ctx.font = "600 16px Inter, Arial, sans-serif";
  ctx.fillText(title, 20, 26);
  ctx.drawImage(chart.canvas, 0, 40);
  ctx.fillStyle = token("--muted");
  ctx.font = "11px Inter, Arial, sans-serif";
  ctx.fillText("PITWALL · Monthly explorer", 20, canvas.height - 10);
  download(canvas.toDataURL("image/png"), filename);
}

/* ---------- Analysis panels (also used side by side in Compare) ---------- */

/** Builds one panel in `container`: its controls, chart and table. Returns its state. */
function createPanel(container, n) {
  const all = seasons();
  const el = document.createElement("section");
  el.className = "panel month-panel";
  el.setAttribute("aria-label", n ? `Panel ${n}` : "Analysis");
  el.innerHTML = `
    <form class="panel-controls">
      <label class="field"><span>Season</span><select data-role="season">${options(all)}</select></label>
      <label class="field"><span>Month</span><select data-role="month">${options(["all", ...MONTHS.map((_, i) => i + 1)], ["All months", ...MONTHS])}</select></label>
      <label class="field"><span>Team</span><select data-role="team"></select></label>
      <fieldset class="field">
        <legend>Chart</legend>
        <div class="seg">
          <label><input type="radio" name="type-${n}" value="bar" checked />Bar</label><label><input type="radio" name="type-${n}" value="line" />Line</label>
        </div>
      </fieldset>
    </form>
    <div class="month-body">
      <div class="month-chart">
        <div class="panel-head">
          <div><h2 data-role="title"></h2><p class="panel-subtitle" data-role="subtitle"></p></div>
          <div class="panel-tools">
            <button class="small-button" type="button" data-role="csv">CSV</button>
            <button class="small-button" type="button" data-role="png">PNG</button>
          </div>
        </div>
        <div class="chart-wrap"><canvas role="img"></canvas></div>
      </div>
      <div class="month-table table-scroll" tabindex="0" role="region" aria-label="Standings">
        <table class="standings">
          <thead><tr><th scope="col">#</th><th scope="col">Constructor</th><th scope="col">Points</th></tr></thead>
          <tbody></tbody>
        </table>
      </div>
    </div>`;
  container.appendChild(el);

  const panel = { el, chart: null, list: [], title: "" };
  const field = (role) => el.querySelector(`[data-role="${role}"]`);
  // Compare panels start on different seasons, so they show something to compare.
  field("season").value = all[Math.min(n ? n - 1 : 0, all.length - 1)];
  const fillTeams = () => {
    const season = Number(field("season").value);
    field("team").innerHTML = options(
      ["all", ...teamsIn(rows.filter((r) => r.season === season))],
      ["All teams", ...teamsIn(rows.filter((r) => r.season === season))],
    );
  };
  fillTeams();
  el.querySelector("form").addEventListener("submit", (event) =>
    event.preventDefault(),
  );
  el.querySelector("form").addEventListener("change", (event) => {
    if (event.target === field("season")) fillTeams();
    drawPanel(panel);
  });
  field("csv").addEventListener("click", () =>
    downloadCsv(panel.list, `pitwall-monthly-${field("season").value}.csv`),
  );
  field("png").addEventListener(
    "click",
    () =>
      panel.chart &&
      downloadPng(
        panel.chart,
        panel.title,
        `pitwall-monthly-${field("season").value}.png`,
      ),
  );
  panel.field = field;
  drawPanel(panel);
  return panel;
}

/** Draws a panel's table and chart for its current season, month, team and chart type. */
function drawPanel(panel) {
  const { field, el } = panel;
  const season = Number(field("season").value);
  const month = field("month").value;
  const team = field("team").value;
  const asLine = el.querySelector('input[value="line"]').checked;
  let list = rows.filter((r) => r.season === season);
  if (month !== "all") list = list.filter((r) => r.month === Number(month));
  const sorted = standings(list);
  panel.list = sorted;

  const period =
    month === "all" ? `${season}` : `${MONTHS[month - 1]} ${season}`;
  panel.title = asLine ? `Championship battle, ${period}` : `Points, ${period}`;
  field("title").textContent = panel.title;
  field("subtitle").textContent = asLine
    ? "Cumulative points, month by month. Point at a line to pick it out."
    : team === "all"
      ? "Points scored by each team"
      : `${team} highlighted`;

  el.querySelector("tbody").innerHTML = sorted.length
    ? sorted
        .map(
          (s, i) =>
            `<tr${s.team === team ? ' class="is-picked"' : ""}><td class="rank">${String(i + 1).padStart(2, "0")}</td><td>${escape(s.team)}</td><td class="points">${number(s.points)}</td></tr>`,
        )
        .join("")
    : '<tr><td colspan="3" class="muted">No points scored in this period.</td></tr>';

  panel.chart?.destroy();
  const canvas = el.querySelector("canvas");
  canvas.setAttribute("aria-label", panel.title);
  if (!asLine) {
    const picked = token("--accent"),
      others = token("--line");
    panel.chart = new Chart(canvas, {
      type: "bar",
      data: {
        labels: sorted.map((s) => s.team),
        datasets: [
          {
            label: "Points",
            data: sorted.map((s) => s.points),
            borderRadius: 4,
            backgroundColor: sorted.map((s) =>
              team === "all" || s.team === team ? picked : others,
            ),
          },
        ],
      },
      options: chartOptions(),
    });
    return;
  }
  // Cumulative points after each month, one line per team (or just the chosen one).
  const dates = [...new Set(list.map((r) => r.date))].sort();
  const palette = PALETTE[theme()];
  const teams = team === "all" ? sorted.map((s) => s.team) : [team];
  const datasets = teams.map((t, i) => {
    let total = 0;
    const colour = palette[i % palette.length];
    return {
      label: t,
      borderColor: colour,
      backgroundColor: colour,
      colour,
      borderWidth: 2.5,
      tension: 0.15,
      pointRadius: 3,
      pointHoverRadius: 6,
      data: dates.map(
        (d) =>
          (total += list
            .filter((r) => r.date === d && r.team === t)
            .reduce((s, r) => s + r.points, 0)),
      ),
    };
  });
  panel.chart = new Chart(canvas, {
    type: "line",
    data: {
      labels: dates.map((d) => MONTHS[Number(d.slice(5, 7)) - 1].slice(0, 3)),
      datasets,
    },
    options: chartOptions({
      interaction: { mode: "dataset", intersect: true },
      plugins: {
        legend: { display: false },
        tooltip: {
          mode: "nearest",
          intersect: true,
          backgroundColor: token("--ink"),
          titleColor: token("--bg"),
          bodyColor: token("--bg"),
          padding: 10,
          cornerRadius: 10,
        },
      },
      // Pointing at a line brings it forward and fades the others.
      onHover: (event, active, chart) => {
        const hovered = active.length ? active[0].datasetIndex : -1;
        if (chart.$hovered === hovered) return;
        chart.$hovered = hovered;
        chart.data.datasets.forEach((d, i) => {
          const faded = hovered !== -1 && i !== hovered;
          d.borderColor = faded ? `${d.colour}26` : d.colour;
          d.borderWidth = hovered === i ? 4 : faded ? 1 : 2.5;
        });
        chart.update("none");
      },
    }),
  });
}

/* ---------- Views ---------- */

function setComparePanels(count) {
  panels.filter((p) => p.compare).forEach((p) => p.chart?.destroy());
  panels = panels.filter((p) => !p.compare);
  $("compare-grid").innerHTML = "";
  for (let n = 1; n <= count; n++) {
    const panel = createPanel($("compare-grid"), n);
    panel.compare = true;
    panels.push(panel);
  }
}

/** Total points per team between two months (inclusive), optionally for one team. */
function drawHistory() {
  const from =
    Number($("hist-start-year").value) * 12 +
    Number($("hist-start-month").value);
  const to =
    Number($("hist-end-year").value) * 12 + Number($("hist-end-month").value);
  const team = $("hist-team").value;
  const error = $("history-error");
  error.hidden = to >= from;
  if (to < from) {
    error.textContent =
      "The end of the period is before its start. Choose a later “To” month.";
    return;
  }
  let list = rows.filter(
    (r) =>
      r.season * 12 + r.month - 1 >= from && r.season * 12 + r.month - 1 <= to,
  );
  if (team !== "all") list = list.filter((r) => r.team === team);
  historyRows = standings(list).slice(0, team === "all" ? 15 : undefined);

  const label = (y, m) => `${MONTHS[m].slice(0, 3)} ${y}`;
  const title = `Points, ${label($("hist-start-year").value, $("hist-start-month").value)} – ${label($("hist-end-year").value, $("hist-end-month").value)}`;
  $("history-title").textContent = title;
  $("history-table").querySelector("tbody").innerHTML = historyRows.length
    ? historyRows
        .map(
          (s, i) =>
            `<tr><td class="rank">${String(i + 1).padStart(2, "0")}</td><td>${escape(s.team)}</td><td class="points">${number(s.points)}</td></tr>`,
        )
        .join("")
    : '<tr><td colspan="3" class="muted">No points scored in this period.</td></tr>';
  historyChart?.destroy();
  historyChart = new Chart($("history-chart"), {
    type: "bar",
    data: {
      labels: historyRows.map((s) => s.team),
      datasets: [
        {
          label: "Points",
          data: historyRows.map((s) => s.points),
          backgroundColor: token("--accent"),
          borderRadius: 4,
        },
      ],
    },
    options: chartOptions(),
  });
  historyChart.$title = title;
}

/** Shows the view named in the address (#analysis by default). */
function showView() {
  const view = ["analysis", "compare", "history"].includes(
    location.hash.slice(1),
  )
    ? location.hash.slice(1)
    : "analysis";
  for (const name of ["analysis", "compare", "history"])
    $(`view-${name}`).hidden = name !== view;
  document.querySelectorAll(".views a").forEach((a) => {
    if (a.dataset.view === view) a.setAttribute("aria-current", "page");
    else a.removeAttribute("aria-current");
  });
  // Charts drawn while hidden have no size: redraw the ones that just appeared.
  redraw();
}

/** Redraws every visible chart (after a view or theme change). */
function redraw() {
  panels.forEach((p) => !p.el.closest("[hidden]") && drawPanel(p));
  if (!$("view-history").hidden) drawHistory();
}

/* ---------- Start-up ---------- */

async function init() {
  try {
    const response = await fetch("f1_data.csv");
    if (!response.ok) throw new Error("Unable to load f1_data.csv");
    rows = parse(await response.text());
    if (!rows.length) throw new Error("f1_data.csv has no rows");
    if (!window.Chart) throw new Error("The chart library could not load");
    Chart.defaults.font.family = token("--font-sans");

    const latest = rows.reduce((a, b) => (a.date > b.date ? a : b));
    $("data-label").textContent =
      `${rows.length.toLocaleString("en-US")} team-months · ${seasons().at(-1)} to ${MONTHS[latest.month - 1]} ${latest.season}`;

    $("status").hidden = true;
    $("app").hidden = false;

    panels.push(createPanel($("analysis-panel"), 0));
    setComparePanels(2);
    $("compare-form").addEventListener("change", (event) =>
      setComparePanels(Number(event.target.value)),
    );
    $("compare-form").addEventListener("submit", (event) =>
      event.preventDefault(),
    );

    // History: the whole archive by default.
    const years = seasons();
    $("hist-start-year").innerHTML = $("hist-end-year").innerHTML =
      options(years);
    $("hist-start-month").innerHTML = $("hist-end-month").innerHTML = options(
      MONTHS.map((_, i) => i),
      MONTHS,
    );
    $("hist-start-year").value = years.at(-1);
    $("hist-end-year").value = years[0];
    $("hist-end-month").value = 11;
    $("hist-team").innerHTML = options(
      ["all", ...teamsIn(rows)],
      ["All teams", ...teamsIn(rows)],
    );
    $("history-form").addEventListener("submit", (event) => {
      event.preventDefault();
      drawHistory();
    });
    $("history-csv").addEventListener("click", () =>
      downloadCsv(historyRows, "pitwall-monthly-history.csv"),
    );
    $("history-png").addEventListener(
      "click",
      () =>
        historyChart &&
        downloadPng(
          historyChart,
          historyChart.$title,
          "pitwall-monthly-history.png",
        ),
    );

    setupTheme($("theme-toggle"), redraw);
    setupHeader(document.querySelector(".site-header"));
    addEventListener("hashchange", showView);
    showView();
  } catch (error) {
    $("app").hidden = true;
    $("status").hidden = false;
    $("status").className = "status wrap error";
    $("status").textContent = `${error.message}. Reload to retry.`;
  }
}

init();
