"""Download new Formula 1 results into data/raw/ from the Jolpica F1 API.

Jolpica (https://github.com/jolpica/jolpica-f1) is the community successor to the
Ergast API that the CSV files in data/raw/ came from, so its data maps directly
onto those files.

Each season is refreshed as a whole: its rows in the season tables (results,
sprints, qualifying, standings) are rebuilt from the API. That way later changes
such as post-race penalties are picked up too, and running the script twice
changes nothing. Lines of other seasons are kept exactly as they are. New
drivers, constructors, circuits and finishing statuses are added with new IDs.

Only rounds whose results AND championship standings are both published are
included, so a half-published weekend is simply picked up on the next run.
Lap times and pit stops are not downloaded; the app does not use them.

Usage:
    python3 scripts/update_results.py                 # latest season in the data -> current season
    python3 scripts/update_results.py --seasons 2026  # chosen seasons only
"""

import argparse
import csv
import io
import json
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
API = "https://api.jolpi.ca/ergast/f1"
PAGE = 100  # the API's maximum page size
NULL = "\\N"  # how the CSV files mark a missing value

# Files whose text columns are written in double quotes (the others are unquoted).
QUOTED = {"circuits", "constructors", "drivers", "status", "seasons"}


# ---------------------------------------------------------------- API access


def get(path, params=None, attempts=5):
    """GET one API page as JSON, waiting and retrying on rate limits or errors."""
    query = "&".join(f"{k}={v}" for k, v in (params or {}).items())
    url = f"{API}/{path}.json" + (f"?{query}" if query else "")
    request = urllib.request.Request(url, headers={"User-Agent": "pitwall-results-updater"})
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                time.sleep(0.3)  # stay well under the API's burst limit (4 requests a second)
                return json.load(response)["MRData"]
        except urllib.error.HTTPError as error:
            if error.code not in (429, 500, 502, 503, 504) or attempt == attempts - 1:
                raise
        except urllib.error.URLError:
            if attempt == attempts - 1:
                raise
        time.sleep(2 ** (attempt + 1))  # 2, 4, 8, 16 seconds


def get_races(path):
    """All races of a paged endpoint (results, sprint, qualifying), merged by round.

    A page holds up to 100 result rows, so one race can be split across two pages.
    """
    races, offset = {}, 0
    while True:
        data = get(path, {"limit": PAGE, "offset": offset})
        for race in data["RaceTable"]["Races"]:
            key = int(race["round"])
            merged = races.setdefault(key, {**race, "rows": []})
            for field in ("Results", "SprintResults", "QualifyingResults"):
                merged["rows"].extend(race.get(field, []))
        offset += PAGE
        if offset >= int(data["total"]):
            return races


def get_standings(season, round_, kind):
    """Constructor or driver standings after one round ([] if not published yet)."""
    lists = get(f"{season}/{round_}/{kind}Standings")["StandingsTable"]["StandingsLists"]
    return lists[0][f"{'Constructor' if kind == 'constructor' else 'Driver'}Standings"] if lists else []


# ---------------------------------------------------------------- CSV files


class Table:
    """One CSV file: keeps the original text of every line, so untouched rows are
    written back exactly as they were and the git diff only shows real changes."""

    def __init__(self, directory, name):
        self.name, self.path = name, directory / f"{name}.csv"
        lines = self.path.read_text(encoding="utf-8-sig").splitlines()
        self.header = next(csv.reader([lines[0]]))
        self.lines = [line for line in lines[1:] if line.strip()]
        self.rows = [dict(zip(self.header, next(csv.reader([line])))) for line in self.lines]

    def next_id(self, column, ignore=lambda row: False):
        """The next free ID, not counting rows that are about to be replaced."""
        ids = [int(r[column]) for r in self.rows if not ignore(r) and r[column].isdigit()]
        return max(ids, default=0) + 1

    def format(self, row):
        """One CSV line in this file's style; missing columns become \\N."""
        values = []
        for column in self.header:
            value = row.get(column)
            value = NULL if value is None or value == "" else str(value)
            if self.name in QUOTED and value != NULL and not is_number(value):
                value = '"' + value.replace('"', '""') + '"'
            elif self.name not in QUOTED and any(c in value for c in ',"\n'):
                value = '"' + value.replace('"', '""') + '"'
            values.append(value)
        return ",".join(values)

    def replace(self, remove, new_rows):
        """Drop rows where remove(row) is true and append new_rows."""
        kept = [(line, row) for line, row in zip(self.lines, self.rows) if not remove(row)]
        self.lines = [line for line, _ in kept] + [self.format(r) for r in new_rows]
        self.rows = [row for _, row in kept] + [
            {c: str(r.get(c) if r.get(c) not in (None, "") else NULL) for c in self.header}
            for r in new_rows
        ]

    def save(self):
        text = ",".join(self.header) + "\n" + "\n".join(self.lines) + "\n"
        old = self.path.read_text(encoding="utf-8-sig")
        if text != old:
            self.path.write_text(text, encoding="utf-8")
            return True
        return False


def is_number(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


def number(value):
    """API numbers as CSV text: 25.0 -> "25", 0.5 -> "0.5"."""
    value = float(value)
    return str(int(value)) if value.is_integer() else str(value)


def clock(value):
    """API times end in Z ("13:00:00Z"); the CSV files store them without it."""
    return value.rstrip("Z") if value else None


# ---------------------------------------------------------------- lookups for IDs


class Ids:
    """Maps API references ("mclaren", "norris", "Finished") to the numeric IDs used
    in the CSV files, adding a new row (with a new ID) the first time one appears."""

    def __init__(self, tables):
        self.t = tables
        self.constructors = {r["constructorRef"]: int(r["constructorId"]) for r in tables["constructors"].rows}
        self.drivers = {r["driverRef"]: int(r["driverId"]) for r in tables["drivers"].rows}
        self.circuits = {r["circuitRef"]: int(r["circuitId"]) for r in tables["circuits"].rows}
        self.status = {r["status"]: int(r["statusId"]) for r in tables["status"].rows}
        self.added = defaultdict(list)

    def _add(self, table, key_map, key, row, id_column):
        new_id = self.t[table].next_id(id_column)
        row[id_column] = new_id
        self.t[table].replace(lambda r: False, [row])
        key_map[key] = new_id
        self.added[table].append(key)
        return new_id

    def constructor(self, c):
        ref = c["constructorId"]
        return self.constructors.get(ref) or self._add(
            "constructors", self.constructors, ref,
            dict(constructorRef=ref, name=c["name"], nationality=c["nationality"], url=c.get("url")),
            "constructorId",
        )

    def driver(self, d):
        ref = d["driverId"]
        return self.drivers.get(ref) or self._add(
            "drivers", self.drivers, ref,
            dict(driverRef=ref, number=d.get("permanentNumber"), code=d.get("code"),
                 forename=d["givenName"], surname=d["familyName"], dob=d.get("dateOfBirth"),
                 nationality=d.get("nationality"), url=d.get("url")),
            "driverId",
        )

    def circuit(self, c):
        ref = c["circuitId"]
        location = c.get("Location", {})
        return self.circuits.get(ref) or self._add(
            "circuits", self.circuits, ref,
            dict(circuitRef=ref, name=c["circuitName"], location=location.get("locality"),
                 country=location.get("country"), lat=location.get("lat"), lng=location.get("long"),
                 url=c.get("url")),
            "circuitId",
        )

    def status_id(self, text):
        return self.status.get(text) or self._add(
            "status", self.status, text, dict(status=text), "statusId"
        )


# ---------------------------------------------------------------- one season


def session(race, *names):
    """Date and time of the first session in `names` that the race has."""
    for name in names:
        if name in race:
            return race[name].get("date"), clock(race[name].get("time"))
    return None, None


def result_row(r, race_id, ids):
    """One results.csv / sprint_results.csv row from an API result."""
    position_text = r["positionText"]
    fastest = r.get("FastestLap", {})
    return dict(
        raceId=race_id,
        driverId=ids.driver(r["Driver"]),
        constructorId=ids.constructor(r["Constructor"]),
        number=r.get("number"),
        grid=r.get("grid"),
        # Only classified finishers have a position; others are R, D, W, N, F or E.
        position=position_text if position_text.isdigit() else None,
        positionText=position_text,
        positionOrder=r["position"],
        points=number(r.get("points", 0)),
        laps=r.get("laps"),
        time=r.get("Time", {}).get("time"),
        milliseconds=r.get("Time", {}).get("millis"),
        fastestLap=fastest.get("lap"),
        rank=fastest.get("rank"),
        fastestLapTime=fastest.get("Time", {}).get("time"),
        fastestLapSpeed=fastest.get("AverageSpeed", {}).get("speed"),
        statusId=ids.status_id(r["status"]),
    )


def fetch_season(season, tables, ids):
    """Download one season and rebuild its rows. Returns the rounds included."""
    schedule = get(f"{season}", {"limit": PAGE})["RaceTable"]["Races"]
    if not schedule:
        raise SystemExit(f"No {season} calendar in the API.")
    results = get_races(f"{season}/results")
    sprints = get_races(f"{season}/sprint")
    qualifying = get_races(f"{season}/qualifying")

    # Rounds count only when both results and constructor standings are published.
    standings, driver_standings = {}, {}
    for round_ in sorted(results):
        constructor_table = get_standings(season, round_, "constructor")
        if not constructor_table or len(results[round_]["rows"]) < 10:
            break  # this round (and later ones) are not fully published yet
        standings[round_] = constructor_table
        driver_standings[round_] = get_standings(season, round_, "driver")
    rounds = sorted(standings)

    # Races: keep each round's ID if the season is already in races.csv.
    races_table = tables["races"]
    existing = {int(r["round"]): int(r["raceId"]) for r in races_table.rows if r["year"] == str(season)}
    in_season = lambda row: row.get("year") == str(season)
    next_race_id = races_table.next_id("raceId", in_season)
    race_rows, race_ids = [], {}
    for race in schedule:
        round_ = int(race["round"])
        race_id = existing.get(round_)
        if race_id is None:
            race_id, next_race_id = next_race_id, next_race_id + 1
        race_ids[round_] = race_id
        fp1 = session(race, "FirstPractice")
        fp2 = session(race, "SecondPractice", "SprintQualifying", "SprintShootout")
        fp3 = session(race, "ThirdPractice")
        quali = session(race, "Qualifying")
        sprint = session(race, "Sprint")
        race_rows.append(dict(
            raceId=race_id, year=season, round=round_, circuitId=ids.circuit(race["Circuit"]),
            name=race["raceName"], date=race["date"], time=clock(race.get("time")), url=race.get("url"),
            fp1_date=fp1[0], fp1_time=fp1[1], fp2_date=fp2[0], fp2_time=fp2[1],
            fp3_date=fp3[0], fp3_time=fp3[1], quali_date=quali[0], quali_time=quali[1],
            sprint_date=sprint[0], sprint_time=sprint[1],
        ))
    races_table.replace(in_season, sorted(race_rows, key=lambda r: r["round"]))

    season_race_ids = {str(i) for i in race_ids.values()} | {str(i) for i in existing.values()}
    in_races = lambda row: row.get("raceId") in season_race_ids

    def rebuild(table_name, id_column, make_rows):
        table = tables[table_name]
        first_id = table.next_id(id_column, in_races)
        rows = make_rows()
        for i, row in enumerate(rows):
            row[id_column] = first_id + i
        table.replace(in_races, rows)

    rebuild("results", "resultId", lambda: [
        result_row(r, race_ids[n], ids) for n in rounds for r in results[n]["rows"]])
    rebuild("sprint_results", "resultId", lambda: [
        result_row(r, race_ids[n], ids) for n in rounds if n in sprints for r in sprints[n]["rows"]])
    rebuild("qualifying", "qualifyId", lambda: [
        dict(raceId=race_ids[n], driverId=ids.driver(q["Driver"]), constructorId=ids.constructor(q["Constructor"]),
             number=q.get("number"), position=q.get("position"), q1=q.get("Q1"), q2=q.get("Q2"), q3=q.get("Q3"))
        for n in rounds if n in qualifying for q in qualifying[n]["rows"]])

    # Constructor points per race: Grand Prix plus sprint, the convention the
    # importer expects (it subtracts the sprint points again).
    def constructor_results():
        rows = []
        for n in rounds:
            points = defaultdict(float)
            for r in results[n]["rows"] + (sprints[n]["rows"] if n in sprints else []):
                points[ids.constructor(r["Constructor"])] += float(r.get("points", 0))
            rows += [dict(raceId=race_ids[n], constructorId=c, points=number(p), status=None)
                     for c, p in points.items()]
        return rows

    rebuild("constructor_results", "constructorResultsId", constructor_results)
    rebuild("constructor_standings", "constructorStandingsId", lambda: [
        dict(raceId=race_ids[n], constructorId=ids.constructor(s["Constructor"]), points=number(s["points"]),
             position=s.get("position", s.get("positionText")), positionText=s["positionText"], wins=s["wins"])
        for n in rounds for s in standings[n]])
    rebuild("driver_standings", "driverStandingsId", lambda: [
        dict(raceId=race_ids[n], driverId=ids.driver(s["Driver"]), points=number(s["points"]),
             position=s.get("position", s.get("positionText")), positionText=s["positionText"], wins=s["wins"])
        for n in rounds for s in driver_standings[n]])

    # Make sure the season itself is listed.
    seasons = tables["seasons"]
    if not any(r["year"] == str(season) for r in seasons.rows):
        seasons.replace(lambda r: False, [dict(
            year=season, url=f"https://en.wikipedia.org/wiki/{season}_Formula_One_World_Championship")])
    return rounds, len(schedule)


def default_seasons(tables):
    """From the latest season that already has results up to the current season."""
    raced = {r["raceId"] for r in tables["results"].rows}
    latest = max(int(r["year"]) for r in tables["races"].rows if r["raceId"] in raced)
    current = int(get("current", {"limit": 1})["RaceTable"]["season"])
    return list(range(latest, max(current, latest) + 1))


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--raw-dir", type=Path, default=ROOT / "data/raw")
    parser.add_argument("--seasons", type=int, nargs="*", help="Seasons to refresh (default: latest -> current)")
    args = parser.parse_args()

    names = ["races", "circuits", "constructors", "drivers", "status", "seasons", "results",
             "sprint_results", "qualifying", "constructor_results", "constructor_standings", "driver_standings"]
    tables = {name: Table(args.raw_dir, name) for name in names}
    ids = Ids(tables)
    seasons = args.seasons or default_seasons(tables)
    for season in seasons:
        rounds, scheduled = fetch_season(season, tables, ids)
        print(f"{season}: {len(rounds)} of {scheduled} rounds with results")
    for table, keys in ids.added.items():
        print(f"New {table}: {', '.join(map(str, keys))}")
    changed = [name for name, table in tables.items() if table.save()]
    print("Updated:", ", ".join(changed) if changed else "nothing (already up to date)")


if __name__ == "__main__":
    main()
