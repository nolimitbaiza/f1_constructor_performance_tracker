# Pitwall — F1 Constructor Performance Tracker

Explore Formula 1 constructor performance from 1958 to the latest race weekend. Pitwall combines an interactive dashboard, historical constructor profiles, team comparisons, and automatically calculated insights with an Express REST API and Python/PostgreSQL data pipeline.

**[Project demo](https://nolimitbaiza.github.io/f1_constructor_performance_tracker/)**

![Pitwall constructor dashboard](docs/dashboard.png)

## Features

- **Overview:** season and constructor filters, official standings, championship points, Grand Prix wins, podiums, and interactive points progression.
- **Constructor profiles:** searchable teams, individual profile URLs, historical totals, season-by-season records, and best/worst complete seasons.
- **Comparison:** compare two to four constructors using points, wins, podiums, average finishes, and historical points shares. Export results as CSV and charts as PNG.
- **Insights:** constructor rankings, championship title counts, points shares, and observations calculated from each season.
- **Two points systems:** switch between the official points and every race re-scored with today's (2025) system, to compare eras like with like. Re-scored tables show each team's official position and how far it moved.
- **Responsive interface:** desktop and mobile layouts, keyboard-accessible controls, labelled charts, and readable data tables.
- **Backend:** validated REST filters, structured errors, PostgreSQL storage, and repeatable transactional Python imports.
- **Monthly explorer:** the original monthly dashboard remains available at `docs/monthly.html`.

## On baizakisaev.com

Pitwall is also published at [baizakisaev.com/pitwall](https://baizakisaev.com/pitwall/), with its case study at [baizakisaev.com/pitwall-case-study](https://baizakisaev.com/pitwall-case-study). The site itself lives in the [portfolio repository](https://github.com/nolimitbaiza/portfolio), which copies the files listed in `publish.json` every hour: the app in `docs/`, and the case study and its pictures in `site/`. The case study uses the portfolio's shared styles, so preview it through the portfolio's local server (see its README). `scripts/build_site_media.py` records the case study's video and screenshots from the live dashboard.

## Live demo and local backend

The GitHub Pages demo runs entirely in the browser using the bundled dataset. It does **not** host the Node.js server or PostgreSQL. Run the project locally to use the REST API, and configure a database to use PostgreSQL storage.

The data is updated automatically after every race weekend (see [Automatic updates](#automatic-updates)). The app opens on the latest complete season; a season still in progress is marked as partial.

## Technologies

| Layer | Technologies |
| --- | --- |
| Frontend | HTML, CSS, JavaScript ES modules, Chart.js |
| REST API | Node.js, Express, node-postgres |
| Database | PostgreSQL |
| Application ETL | Python standard library, psycopg |
| Original monthly pipeline | Pandas, DuckDB, PyArrow, Matplotlib |
| Original monthly UI | Bootstrap, PapaParse, Chart.js |
| Demo hosting | GitHub Pages |
| Tests | Node.js test runner, Python unittest |

## Quickest way to try it

Double-click **`Pitwall.html`** in the project folder. It opens in your browser and works offline, with nothing to install: the stylesheet, Chart.js, the dataset and the app code are all bundled into that one file. (The *Monthly explorer* link in it opens the hosted copy online.)

`Pitwall.html` is generated from `docs/`. After changing the app, rebuild it with `npm run standalone`.

## Run locally

Requirements: **Node.js 22+** and **Python 3.10+** for data generation and tests.

```bash
git clone https://github.com/nolimitbaiza/f1_constructor_performance_tracker.git
cd f1_constructor_performance_tracker
npm ci
npm start
```

Open [http://localhost:3000](http://localhost:3000).

Without `DATABASE_URL`, the API uses `docs/data.json`; no database is required to try the application. Chart.js is vendored with its license. For a frontend-only preview:

```bash
python3 -m http.server 8000 --directory docs
```

Open [http://localhost:8000](http://localhost:8000). The frontend loads the snapshot when the API is unavailable (404).

## PostgreSQL setup

Create a PostgreSQL database, then install the importer dependencies:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-app.txt
export DATABASE_URL='postgresql://postgres:YOUR_PASSWORD@localhost:5432/f1'
.venv/bin/python scripts/build_app_data.py --database
npm start
```

Use your own database credentials. Node also reads `.env` based on `.env.example`; the Python importer expects `DATABASE_URL` to be exported.

The importer creates a separate `f1` schema containing constructors, seasons, races, constructor-level race results, standings, and source metadata. It upserts supplied records in one transaction; repeated imports do not create duplicates. Records absent from a later source export are not deleted automatically.

The API reads a consistent database snapshot and caches it for 30 seconds. Database failures return an error instead of falling back silently to the bundled snapshot. The server binds to loopback for local use.

## Automatic updates

A GitHub Actions workflow (`.github/workflows/update-results.yml`) runs on Monday and Wednesday mornings (UTC). It downloads new results from the [Jolpica F1 API](https://github.com/jolpica/jolpica-f1), the community successor to the Ergast API that `data/raw/` came from. It then rebuilds `docs/data.json`, `Pitwall.html` and the monthly explorer's CSV, runs the tests and commits the update, which GitHub Pages publishes. When there are no new results, nothing is committed. To run it by hand: **Actions → Update race results → Run workflow** (optionally naming seasons).

Each run refreshes whole seasons — by default from the newest season in the data to the current one — so later corrections such as post-race penalties are picked up too. A round is added only once both its results and its championship standings are published. Rows of other seasons stay exactly as they are. Lap times and pit stops are not downloaded; the app does not use them.

To update locally:

```bash
npm run update
npm run data
npm run standalone
npm test
```

`npm run update` takes optional seasons: `npm run update -- --seasons 2025 2026`. The importer validates keys and result counts before atomically replacing `docs/data.json`. Restart a snapshot-mode server after updating the file. To update PostgreSQL too, rerun the importer with `--database`. The original optional FastF1 loader is separate and does not populate the application's `f1` schema.

Tests cover known season totals, exclusions, partial seasons, re-scoring, the results updater (against a fake API), progression, CSV escaping, API validation, missing endpoints, storage failures, and ETL integrity. Desktop/mobile flows, CSV/PNG downloads, static hosting, and repeated PostgreSQL imports have also been checked locally.

## API

| Route | Purpose |
| --- | --- |
| `/api/health` | Storage health and data source |
| `/api/seasons` | Available seasons and completeness |
| `/api/constructors` | Constructor identities |
| `/api/constructors/6` | Ferrari profile and history |
| `/api/dashboard?season=2024&constructors=1,6` | Filtered metrics and progression |
| `/api/standings?season=2024` | Constructor rankings |
| `/api/comparison?season=2024&constructors=1,6` | Compare at least two constructors |
| `/api/analytics?season=2024` | Rankings, progression and observations |
| `/api/standings?season=1988&scoring=rescored` | Any season route or `/api/constructors/:id` re-scored with 2025 points |
| `/api/comparison?season=2024&constructors=1,6&format=csv` | Export comparison results |
| `/api/data` | Complete dataset used by the frontend |

`scoring` is `official` (default) or `rescored`. Invalid filters return HTTP 400; missing resources return 404. Unavailable storage returns 500 without exposing connection details.

## Project structure

```text
Pitwall.html              Self-contained offline version (double-click to open)
PROJECT_DOCUMENTATION.txt Project documentation for submission
docs/
  index.html              Main application
  logo.svg                Logo and favicon
  app.css                 Responsive styles
  app.js                  Views, controls, charts and exports
  analytics.js            Shared statistics logic
  data.json               Generated race snapshot
  monthly.html            Original monthly dashboard
  f1_data.csv             Original monthly dataset
  vendor/                 Chart.js and its license
server/
  app.js                  API routes and validation
  index.js                Server and database access
database/schema.sql       PostgreSQL schema
scripts/
  build_app_data.py       Snapshot and PostgreSQL importer
  build_standalone.js     Builds Pitwall.html from docs/
  build_site_media.py     Records the case study's video and screenshots
  update_results.py       Downloads new results from the Jolpica F1 API
site/                     Case study and pictures published on baizakisaev.com
publish.json              What baizakisaev.com copies from this repository
.github/workflows/
  update-results.yml      Automatic updates after race weekends
  load_fastf1_to_postgres.py  Original optional FastF1 loader
src/tracker/              Original monthly ETL and chart generation
data/raw/                 Source CSV tables
data/bronze/               Joined monthly pipeline data
data/silver/               Validated monthly pipeline data
data/gold/                 Monthly aggregates
reports/charts/            Generated monthly charts
tests/
  analytics.test.js       Statistics and REST API tests
  test_app_data.py         ETL integrity tests
```

## Statistics and limitations

- The archive starts in 1958, the first constructors' championship season.
- Championship totals and progression use `constructor_standings.csv`, rather than sums of driver points.
- Entries marked `E` are excluded: championship points are normalized to zero and tables display `EX`. Source standings may reflect retrospective exclusions rather than the standings displayed on race day.
- Grand Prix wins and podiums exclude sprints. Championship points include sprint contributions; the importer avoids counting them twice.
- Average finish uses classified `position` values only; unclassified entries are excluded.
- Race appearances count distinct constructor/race pairs, not driver starts.
- Best/worst seasons use championship position, then points share. Incomplete seasons are excluded from those rankings and title counts.
- Raw official points are not directly comparable across scoring eras. Points share provides context, and the 2025 points system removes the scoring differences.
- **Re-scored points (2025 system):** 25-18-15-12-10-8-6-4-2-1 for the top ten in every Grand Prix and 8-7-6-5-4-3-2-1 for the top eight in sprints, with no fastest-lap point. Every race counts and both cars score (older rules often counted only a team's best car or best results). Points follow the official classification, including disqualifications; shortened races still score full points. A shared car scores once. The Indianapolis 500 (1958–1960) stays out, as it never counted for constructors. Teams excluded from a championship stay excluded. Ties are split by wins, then podiums, then the official position. Re-scoring 2025 reproduces the official 2025 standings exactly.
- Renamed constructors remain separate source identities. Completeness is measured against the supplied race schedule.

## Original monthly pipeline

```bash
pip install -r requirements.txt
python -m src.tracker.pipeline
python -m src.tracker.report --month 2021-12
```

The monthly pipeline joins raw CSV files, validates race-team records, aggregates monthly points with DuckDB, and exports the CSV consumed by `docs/monthly.html`.

## Team and contributions

This is a shared project by [nolimitbaiza](https://github.com/nolimitbaiza) and [bekuuu1](https://github.com/bekuuu1), developed in [our shared repository](https://github.com/nolimitbaiza/f1_constructor_performance_tracker).

| Collaborator | Contributions |
| --- | --- |
| Baizak Isaev (`nolimitbaiza`) | Initial project, source dataset integration, monthly ETL pipeline, and original dashboard. |
| `bekuuu1` | Pitwall dashboard expansion, constructor profiles, comparisons, analytics, Express API, PostgreSQL importer, tests, and updated documentation, with Codex assistance. |

We submit changes on feature branches and review pull requests before merging into the default `master` branch. The original monthly pipeline and dashboard are retained alongside the new application.

## License

[MIT](LICENSE). Original copyright notices are retained. Chart.js includes its own license under `docs/vendor/`.
