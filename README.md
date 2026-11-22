# Pitwall — F1 Constructor Performance Tracker

Explore Formula 1 constructor performance from 1958 through August 2025. Pitwall combines an interactive dashboard, historical constructor profiles, team comparisons, and automatically calculated insights with an Express REST API and Python/PostgreSQL data pipeline.

**[Project demo](https://nolimitbaiza.github.io/f1_constructor_performance_tracker/)**

![Pitwall constructor dashboard](docs/dashboard.png)

## Features

- **Overview:** season and constructor filters, official standings, championship points, Grand Prix wins, podiums, and interactive points progression.
- **Constructor profiles:** searchable teams, individual profile URLs, historical totals, season-by-season records, and best/worst complete seasons.
- **Comparison:** compare two to four constructors using points, wins, podiums, average finishes, and historical points shares. Export results as CSV and charts as PNG.
- **Insights:** constructor rankings, championship title counts, points shares, and observations calculated from each season.
- **Responsive interface:** desktop and mobile layouts, keyboard-accessible controls, labelled charts, and readable data tables.
- **Backend:** validated REST filters, structured errors, PostgreSQL storage, and repeatable transactional Python imports.
- **Monthly explorer:** the original monthly dashboard remains available at `docs/monthly.html`.

## Live demo and local backend

The GitHub Pages demo runs entirely in the browser using the bundled dataset. It does **not** host the Node.js server or PostgreSQL. Run the project locally to use the REST API, and configure a database to use PostgreSQL storage.

The dataset ends on **August 31, 2025**: 15 of 24 scheduled races in 2025. The app defaults to 2024, the latest complete season. This is a historical archive, not a live feed.

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

## Update data and run checks

Replace the corresponding CSV exports in `data/raw/` from the same source, then run:

```bash
npm run data
npm test
```

The importer validates keys and result counts before atomically replacing `docs/data.json`. Restart a snapshot-mode server after updating the file. To update PostgreSQL too, rerun the importer with `--database`.

The importer does not automatically download new races. The original optional FastF1 loader is separate and does not populate the application's `f1` schema.

Tests cover known season totals, exclusions, partial seasons, progression, CSV escaping, API validation, missing endpoints, storage failures, and ETL integrity. Desktop/mobile flows, CSV/PNG downloads, static hosting, and repeated PostgreSQL imports have also been checked locally.

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
| `/api/comparison?season=2024&constructors=1,6&format=csv` | Export comparison results |
| `/api/data` | Complete dataset used by the frontend |

Invalid filters return HTTP 400; missing resources return 404. Unavailable storage returns 500 without exposing connection details.

## Project structure

```text
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
- Raw points are not directly comparable across scoring eras. Points share provides context but does not remove every difference between eras.
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
