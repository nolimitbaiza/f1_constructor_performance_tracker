import {
  seasonStats,
  progression,
  profile,
  insights,
  csv,
} from "./analytics.js";

const $ = (id) => document.getElementById(id);
const escape = (value) =>
  String(value ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
const number = (value) =>
  value == null
    ? "—"
    : Number(value).toLocaleString("en-US", { maximumFractionDigits: 1 });
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
let data,
  year,
  page = "dashboard",
  selected = null,
  charts = [],
  exportedRows = [];
const color = (id) =>
  colors[data.constructors.find((c) => c.id === id)?.ref] ??
  `hsl(${(id * 137) % 360} 55% 65%)`;
const teamName = (t) =>
  `<a class="team-link" href="#profiles/${t.id}"><span class="team-stripe" style="--team:${color(t.id)}"></span>${escape(t.name)}</a>`;
const metric = (label, value, detail, icon) =>
  `<div class="metric"><div class="metric-label">${label}<span class="metric-icon" aria-hidden="true">${icon}</span></div><div class="metric-value">${value}</div><div class="metric-detail">${detail}</div></div>`;
const panel = (title, subtitle, body, extra = "") =>
  `<section class="panel ${extra}"><div class="panel-head"><div><h2>${title}</h2><p class="panel-subtitle">${subtitle}</p></div></div>${body}</section>`;
const chartPanel = (id, title, subtitle, extra = "") =>
  `<section class="panel ${extra}"><div class="panel-head"><div><h2>${title}</h2><p class="panel-subtitle">${subtitle}</p></div><button class="small-button" data-chart="${id}" aria-label="Download ${title} as PNG">PNG</button></div><div class="chart-wrap"><canvas id="${id}" role="img" aria-label="${title}"></canvas></div><div class="chart-legend" id="${id}-legend"></div></section>`;
function table(rows, compact = false) {
  const max = Math.max(1, ...rows.map((t) => t.points ?? 0));
  return `<div class="table-scroll"><table class="standings ${compact ? "compact" : ""}"><thead><tr><th scope="col">POS</th><th scope="col">CONSTRUCTOR</th><th scope="col">POINTS</th>${compact ? "" : '<th scope="col">WINS</th><th scope="col">PODIUMS</th><th scope="col">RACES</th><th scope="col">AVG FINISH</th>'}</tr></thead><tbody>${rows.map((t) => `<tr><td class="rank">${t.excluded ? "EX" : t.rank == null ? "—" : String(t.rank).padStart(2, "0")}</td><td>${teamName(t)}</td><td class="points">${number(t.points)}${compact ? `<div class="bar-track"><div class="bar-fill" style="width:${Math.max(0, ((t.points ?? 0) / max) * 100)}%;--team:${color(t.id)}"></div></div>` : ""}</td>${compact ? "" : `<td>${t.wins}</td><td>${t.podiums}</td><td>${t.races}</td><td>${number(t.average)}</td>`}</tr>`).join("")}</tbody></table></div>`;
}
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
        legend: { display: false },
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
  const legend = $(`${id}-legend`);
  if (legend)
    legend.innerHTML = datasets
      .map(
        (d, i) =>
          `<button class="small-button" data-toggle-chart="${id}" data-series="${i}" aria-pressed="true"><span class="swatch" style="--team:${Array.isArray(d.borderColor) ? "#aaa" : d.borderColor}"></span>${escape(d.label)}</button>`,
      )
      .join("");
}
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
      pointRadius: labels.length > 30 ? 0 : 2,
      pointHoverRadius: 5,
      tension: 0.18,
      spanGaps: false,
    })),
  );
}
function pointsChart(id, ids) {
  const p = progression(data, year, ids);
  line(
    id,
    p.races.map((r) => `R${r.round} · ${r.name.replace(" Grand Prix", "")}`),
    p.series,
  );
}
function observationList() {
  return `<div class="insight-list">${insights(data, year)
    .map(
      (i, n) =>
        `<article class="insight-row"><div class="insight-number">${String(n + 1).padStart(2, "0")}</div><div><h3>${i.title} <span class="muted">/</span> <strong>${escape(i.value)}</strong></h3><p>${escape(i.detail)}</p></div></article>`,
    )
    .join("")}</div>`;
}
function dashboard() {
  const all = seasonStats(data, year);
  const id = $("constructor").value;
  const teams = id === "all" ? all : all.filter((t) => t.id === Number(id));
  const sum = (key) => teams.reduce((s, t) => s + (t[key] ?? 0), 0);
  const races = data.seasons.find((s) => s.year === year).races;
  exportedRows = teams;
  $("view").innerHTML =
    `<div class="metrics">${metric("Championship points", number(sum("points")), id === "all" ? `Across <em>${all.length} constructors</em>` : "Official championship total", "◈")}${metric("Grand Prix wins", number(sum("wins")), "Race victories · sprints excluded", "⚑")}${metric("Podium finishes", number(sum("podiums")), "Top-three Grand Prix finishes", "▥")}${metric("Grands Prix", number(id === "all" ? races : (teams[0]?.races ?? 0)), `Of ${data.seasons.find((s) => s.year === year).scheduled} scheduled rounds`, "▦")}</div>
  <div class="grid-main">${chartPanel("progress", "Championship progression", "Cumulative official points after each Grand Prix")}${panel("Constructor standings", `${year} season · top five constructors`, table(all.slice(0, 5), true) + '<div class="table-bottom"><span>Explore the teams behind the numbers</span><a href="#profiles">View teams</a></div>')}</div>
  <div class="bottom-grid">${chartPanel("wins", "Who owns the podium?", "Grand Prix wins and podium finishes")}${panel("From the pitwall", "The story behind this season’s numbers", observationList())}</div>
  ${panel("Season breakdown", "Average finish includes classified Grand Prix finishes only.", table(teams))}`;
  pointsChart(
    "progress",
    teams.slice(0, 5).map((t) => t.id),
  );
  makeChart(
    "wins",
    "bar",
    teams.slice(0, 6).map((t) => t.name),
    [
      {
        label: "Wins",
        data: teams.slice(0, 6).map((t) => t.wins),
        backgroundColor: "#ff765e",
        borderColor: "#ff765e",
        borderRadius: 3,
      },
      {
        label: "Podiums",
        data: teams.slice(0, 6).map((t) => t.podiums),
        backgroundColor: "#66768e",
        borderColor: "#66768e",
        borderRadius: 3,
      },
    ],
  );
}
function profiles(id) {
  if (!id) {
    const teams = seasonStats(data, year);
    exportedRows = teams;
    $("view").innerHTML =
      '<label class="muted" for="team-search">Find a constructor across the archive</label><input class="search" id="team-search" type="search" placeholder="Search Ferrari, McLaren, Lotus…"><div class="team-grid" id="team-grid"></div>';
    const draw = (query) => {
      const list = query
        ? data.constructors.filter(
            (t) =>
              t.name.toLowerCase().includes(query.toLowerCase()) &&
              data.results.some((r) => r.constructorId === t.id),
          )
        : teams;
      $("team-grid").innerHTML = list.length
        ? list
            .map(
              (t) =>
                `<a class="team-card" href="#profiles/${t.id}" style="--team:${color(t.id)}"><div class="eyebrow">${escape(t.nationality)} CONSTRUCTOR</div><h2>${escape(t.name)}</h2><p>${query ? "Explore its championship history" : `${year} championship · P${t.rank ?? "—"}`}</p><div class="team-stats"><span>${query ? "Historical profile" : `${number(t.points)} points`}</span><span>${query ? "Season archive" : `${t.wins} wins`}</span></div></a>`,
            )
            .join("")
        : '<p class="empty">No constructors match your search.</p>';
    };
    draw("");
    $("team-search").addEventListener("input", (event) =>
      draw(event.target.value),
    );
    return;
  }
  const p = profile(data, id);
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
  $("view").innerHTML =
    `<section class="profile-banner" style="--team:${color(id)}"><div><div class="eyebrow">CONSTRUCTOR DOSSIER</div><h2>${escape(t.name)}</h2><p>${p.seasons.length} seasons of racing history.<br>Constructor identities are kept separate across name changes.</p></div><div class="profile-bests"><div><span>BEST COMPLETE SEASON</span>${best(p.best)}</div><div><span>WORST COMPLETE SEASON</span>${best(p.worst)}</div></div></section>
  <div class="metrics">${metric("Constructor titles", p.titles, "Completed seasons since 1958", "◈")}${metric("Grand Prix wins", number(p.wins), "Across the available archive", "⚑")}${metric("Podiums", number(p.podiums), "Grand Prix top-three finishes", "▥")}${metric("Race appearances", number(p.races), "Unique constructor race weekends", "▦")}</div>
  <div class="bottom-grid">${chartPanel("history", "Through the seasons", "Share of all championship points · %")}${chartPanel("profile-progress", `${year} points progression`, "Official championship points by round")}</div>
  ${panel(
    "Season-by-season record",
    "Best/worst ranked by championship position, then points share. Partial seasons excluded.",
    `<div class="table-scroll"><table class="standings"><thead><tr><th>SEASON</th><th>POSITION</th><th>POINTS</th><th>SHARE</th><th>WINS</th><th>PODIUMS</th><th>AVG FINISH</th></tr></thead><tbody>${[
      ...p.seasons,
    ]
      .reverse()
      .map(
        (s) =>
          `<tr><td>${s.season}${s.complete ? "" : " *"}</td><td>${s.excluded ? "EX" : `P${s.rank ?? "—"}`}</td><td>${number(s.points)}</td><td>${number(s.share)}%</td><td>${s.wins}</td><td>${s.podiums}</td><td>${number(s.average)}</td></tr>`,
      )
      .join("")}</tbody></table></div>`,
  )}<div class="callout">* Partial season. Raw points are not directly comparable across scoring eras. Points share provides context; it does not remove every difference between eras.</div>`;
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
function comparison() {
  const teams = seasonStats(data, year);
  const validIds = new Set(teams.map((t) => t.id));
  selected =
    selected === null
      ? teams.slice(0, 3).map((t) => t.id)
      : selected.filter((id) => validIds.has(id));
  const chosen = teams.filter((t) => selected.includes(t.id));
  exportedRows = chosen;
  $("view").innerHTML =
    `<section class="panel selection"><p>Choose 2–4 constructors to compare. All teams shown raced in ${year}.</p><div class="team-options">${teams.map((t) => `<label class="team-option" style="--team:${color(t.id)}"><input type="checkbox" value="${t.id}" ${selected.includes(t.id) ? "checked" : ""} ${selected.length >= 4 && !selected.includes(t.id) ? "disabled" : ""}>${escape(t.name)}</label>`).join("")}</div></section><div id="compare-content"></div>`;
  document.querySelectorAll(".team-option input").forEach((input) =>
    input.addEventListener("change", () => {
      selected = [
        ...document.querySelectorAll(".team-option input:checked"),
      ].map((i) => Number(i.value));
      render();
    }),
  );
  if (selected.length < 2) {
    $("compare-content").innerHTML =
      '<div class="empty">Select at least two constructors to see their comparison.</div>';
    return;
  }
  $("compare-content").innerHTML =
    `<div class="bottom-grid">${chartPanel("comparison-points", "Head to head", "Official championship points by round")}${chartPanel("comparison-history", "The longer rivalry", "Share of championship points across seasons · %")}</div>${panel("Side-by-side results", `${year} season · average classified Grand Prix finish`, table(chosen))}<div class="bottom-grid section-gap" style="margin-top:24px">${chartPanel("comparison-wins", "Race wins", "Grand Prix victories in the selected season")}${chartPanel("comparison-podiums", "Podium finishes", "Top-three Grand Prix finishes in the selected season")}</div>`;
  pointsChart("comparison-points", selected);
  const histories = selected.map((id) => profile(data, id));
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
    makeChart(
      `comparison-${field}`,
      "bar",
      chosen.map((t) => t.name),
      [
        {
          label: field === "wins" ? "Wins" : "Podiums",
          data: chosen.map((t) => t[field]),
          backgroundColor: chosen.map((t) => color(t.id)),
          borderColor: "#c6cbd3",
          borderRadius: 3,
        },
      ],
    );
}
function analytics() {
  const teams = seasonStats(data, year);
  exportedRows = teams;
  const top = teams.slice(0, 5);
  const historical = new Map();
  for (const season of data.seasons.filter((s) => s.complete)) {
    const winner = seasonStats(data, season.year).find((t) => t.rank === 1);
    if (winner) historical.set(winner.id, (historical.get(winner.id) ?? 0) + 1);
  }
  const champions = [...historical].sort((a, b) => b[1] - a[1]).slice(0, 8);
  $("view").innerHTML =
    `<div class="bottom-grid">${panel("The season in focus", "Automatically calculated from the selected season", observationList())}${chartPanel("dominance", "An era of their own", "Constructor titles · completed seasons since 1958")}</div><div class="bottom-grid">${chartPanel("share", "Share of the championship", `${year} · percentage of official points`)}${chartPanel("trends", "Performance across seasons", "Selected season’s top five teams · points share %")}</div>${panel("Constructor rankings", "Official championship order, with race-level performance alongside it.", table(teams))}<div class="callout">Championship points and ranks use official standings, including adjustments. Wins and podiums are Grand Prix results; sprint points contribute to championship points. Average finish excludes unclassified results. The latest partial season is not counted as a title.</div>`;
  makeChart(
    "dominance",
    "bar",
    champions.map(([id]) => data.constructors.find((t) => t.id === id).name),
    [
      {
        label: "Titles",
        data: champions.map(([, count]) => count),
        backgroundColor: champions.map(([id]) => color(id)),
        borderColor: "#aeb6c5",
        borderRadius: 3,
      },
    ],
  );
  makeChart(
    "share",
    "bar",
    teams.map((t) => t.name),
    [
      {
        label: "Points share %",
        data: teams.map((t) => t.share),
        backgroundColor: teams.map((t) => color(t.id)),
        borderColor: "#aeb6c5",
        borderRadius: 3,
      },
    ],
  );
  const years = data.seasons
    .filter((s) => s.year <= year && s.year >= year - 14)
    .map((s) => s.year);
  line(
    "trends",
    years,
    top.map((t) => ({
      id: t.id,
      name: t.name,
      values: years.map((y) => seasonStats(data, y, [t.id])[0]?.share ?? null),
    })),
  );
}
function render() {
  charts.forEach((c) => c.destroy());
  charts = [];
  const [route, rawId] = location.hash.slice(1).split("/");
  page = ["dashboard", "profiles", "comparison", "insights"].includes(route)
    ? route
    : "dashboard";
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
  const title = titles[page];
  $("eyebrow").textContent = title[0];
  $("page-title").innerHTML = `${title[1]}<span>.</span>`;
  $("page-description").textContent = title[2];
  $("breadcrumb").textContent = title[3];
  document.querySelectorAll("nav a").forEach((a) => {
    a.classList.toggle("active", a.dataset.page === page);
    if (a.dataset.page === page) a.setAttribute("aria-current", "page");
    else a.removeAttribute("aria-current");
  });
  $("constructor-filter").hidden = page !== "dashboard";
  const season = data.seasons.find((s) => s.year === year);
  $("coverage").innerHTML =
    `<span class="tag ${season.complete ? "" : "partial"}">${season.complete ? "COMPLETE SEASON" : "PARTIAL SEASON"}</span> ${season.races} / ${season.scheduled} ROUNDS`;
  if (page === "dashboard") dashboard();
  if (page === "profiles") profiles(rawId ? Number(rawId) : null);
  if (page === "comparison") comparison();
  if (page === "insights") analytics();
  document.querySelectorAll("[data-chart]").forEach((button) =>
    button.addEventListener("click", () => {
      const chart = charts.find((c) => c.canvas.id === button.dataset.chart);
      chart.options.plugins.legend.display = true;
      chart.options.plugins.legend.labels = { color: "#e2e5e9" };
      chart.update("none");
      const canvas = document.createElement("canvas");
      canvas.width = chart.canvas.width;
      canvas.height = chart.canvas.height + 70;
      const ctx = canvas.getContext("2d");
      ctx.fillStyle = "#191c20";
      ctx.fillRect(0, 0, canvas.width, canvas.height);
      ctx.fillStyle = "#f4f5f7";
      ctx.font = "16px Arial";
      ctx.fillText(
        `${button.closest(".panel").querySelector("h2").textContent} · ${year}`,
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
    }),
  );
  document.querySelectorAll("[data-toggle-chart]").forEach((button) =>
    button.addEventListener("click", () => {
      const chart = charts.find(
        (c) => c.canvas.id === button.dataset.toggleChart,
      );
      const index = Number(button.dataset.series);
      const visible = !chart.isDatasetVisible(index);
      chart.setDatasetVisibility(index, visible);
      button.setAttribute("aria-pressed", String(visible));
      button.style.opacity = visible ? "1" : ".45";
      chart.update();
    }),
  );
}
function updateConstructors() {
  const old = $("constructor").value;
  const teams = seasonStats(data, year);
  $("constructor").innerHTML =
    '<option value="all">All constructors</option>' +
    teams
      .map((t) => `<option value="${t.id}">${escape(t.name)}</option>`)
      .join("");
  $("constructor").value = teams.some((t) => String(t.id) === old)
    ? old
    : "all";
}
function download(url, filename) {
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
}
async function init() {
  try {
    let response = await fetch("api/data");
    if (response.status === 404) response = await fetch("data.json");
    if (!response.ok) throw new Error("Unable to retrieve the dataset");
    data = await response.json();
    if (!data.seasons?.length) throw new Error("No seasons have been imported");
    if (!window.Chart) throw new Error("The chart library could not load");
    year =
      [...data.seasons].reverse().find((s) => s.complete)?.year ??
      data.seasons.at(-1).year;
    $("season").innerHTML = [...data.seasons]
      .reverse()
      .map(
        (s) =>
          `<option value="${s.year}">${s.year}${s.complete ? "" : " · partial"}</option>`,
      )
      .join("");
    $("season").value = year;
    $("data-label").textContent = `ARCHIVE THROUGH ${data.meta.through}`;
    $("archive-range").textContent =
      `${data.seasons[0].year} — ${data.seasons.at(-1).year}`;
    $("footer-coverage").textContent =
      `Results through ${data.meta.through} · Latest season partial · Not live data`;
    updateConstructors();
    $("season").addEventListener("change", () => {
      year = Number($("season").value);
      selected = null;
      updateConstructors();
      render();
    });
    $("constructor").addEventListener("change", render);
    $("export").addEventListener("click", () => {
      const url = URL.createObjectURL(
        new Blob(
          [
            csv(exportedRows, [
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
            ]),
          ],
          { type: "text/csv;charset=utf-8" },
        ),
      );
      download(url, `pitwall-${page}-${year}.csv`);
      setTimeout(() => URL.revokeObjectURL(url), 1000);
    });
    window.addEventListener("hashchange", render);
    $("status").hidden = true;
    $("app").hidden = false;
    render();
  } catch (error) {
    $("app").hidden = true;
    $("status").hidden = false;
    $("status").className = "status error";
    $("status").textContent =
      `${error.message}. Reload to retry, or check the local server.`;
  }
}
init();
