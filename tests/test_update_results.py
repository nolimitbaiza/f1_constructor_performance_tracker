"""Tests for scripts/update_results.py with a fake API, so they never need the network."""
import shutil
import tempfile
import unittest
from pathlib import Path

from scripts import update_results as updater

RAW = Path(__file__).resolve().parents[1] / "data" / "raw"
TABLES = ["races", "circuits", "constructors", "drivers", "status", "seasons", "results",
          "sprint_results", "qualifying", "constructor_results", "constructor_standings", "driver_standings"]

# A tiny made-up season: 2 rounds on the calendar, only round 1 raced so far.
CIRCUIT = {"circuitId": "albert_park", "circuitName": "Albert Park", "url": "u",
           "Location": {"lat": "1", "long": "2", "locality": "Melbourne", "country": "Australia"}}
SCHEDULE = [{"season": "2030", "round": str(n), "raceName": f"Race {n}", "date": f"2030-03-0{n}",
             "time": "05:00:00Z", "url": "u", "Circuit": CIRCUIT} for n in (1, 2)]
TEAMS = [{"constructorId": "mclaren", "name": "McLaren", "nationality": "British"},
         {"constructorId": "newteam", "name": "New Team", "nationality": "Swiss", "url": "u"}]


def driver(n):
    return {"driverId": "norris" if n == 1 else f"rookie{n}", "givenName": "A", "familyName": f"D{n}"}


RESULTS = [{"number": str(n), "position": str(n), "positionText": "R" if n == 10 else str(n),
            "points": str(max(0, 26 - 3 * n)), "grid": str(n), "laps": "50",
            "status": "Retired" if n == 10 else "Finished",
            "Driver": driver(n), "Constructor": TEAMS[n % 2]} for n in range(1, 11)]
STANDINGS = [{"position": "1", "positionText": "1", "points": "80", "wins": "1", "Constructor": TEAMS[1]},
             {"position": "2", "positionText": "2", "points": "40", "wins": "0", "Constructor": TEAMS[0]}]


def fake_get(path, params=None):
    if path == "2030":
        return {"RaceTable": {"Races": SCHEDULE}}
    if path == "2030/results":
        return {"total": "10", "RaceTable": {"Races": [{**SCHEDULE[0], "Results": RESULTS}]}}
    if path in ("2030/sprint", "2030/qualifying"):
        return {"total": "0", "RaceTable": {"Races": []}}
    if path == "2030/1/constructorStandings":
        return {"StandingsTable": {"StandingsLists": [{"ConstructorStandings": STANDINGS}]}}
    if path == "2030/1/driverStandings":
        rows = [{"position": str(n), "positionText": str(n), "points": "1", "wins": "0", "Driver": driver(n)}
                for n in range(1, 11)]
        return {"StandingsTable": {"StandingsLists": [{"DriverStandings": rows}]}}
    return {"StandingsTable": {"StandingsLists": []}, "total": "0", "RaceTable": {"Races": []}}


class UpdaterTests(unittest.TestCase):
    def setUp(self):
        # Small copies of the real files: headers plus a few existing rows.
        self.dir = Path(tempfile.mkdtemp())
        for name in TABLES:
            lines = (RAW / f"{name}.csv").read_text(encoding="utf-8-sig").splitlines()
            keep = {"constructors": 2, "drivers": 2, "status": 2, "circuits": 2, "seasons": 2}.get(name, 1)
            (self.dir / f"{name}.csv").write_text("\n".join(lines[:keep]) + "\n", encoding="utf-8")
        # An existing results line with its own formatting, which must survive untouched.
        self.untouched = '1,99,1,1,22,1,1,"1",1,10,58,"1:34:50.616",5690616,39,2,1:27.452,218.3,1'
        with open(self.dir / "results.csv", "a", encoding="utf-8") as f:
            f.write(self.untouched + "\n")
        self.original_get = updater.get
        updater.get = fake_get

    def tearDown(self):
        updater.get = self.original_get
        shutil.rmtree(self.dir)

    def run_update(self):
        tables = {name: updater.Table(self.dir, name) for name in TABLES}
        ids = updater.Ids(tables)
        rounds, scheduled = updater.fetch_season(2030, tables, ids)
        return rounds, scheduled, [name for name, table in tables.items() if table.save()]

    def rows(self, name):
        return updater.Table(self.dir, name).rows

    def test_new_season_is_added_and_a_second_run_changes_nothing(self):
        rounds, scheduled, changed = self.run_update()
        self.assertEqual((rounds, scheduled), ([1], 2))  # round 2 is not raced yet
        self.assertIn("results", changed)
        races = [r for r in self.rows("races") if r["year"] == "2030"]
        self.assertEqual([r["round"] for r in races], ["1", "2"])
        results = [r for r in self.rows("results") if r["raceId"] == races[0]["raceId"]]
        self.assertEqual(len(results), 10)
        # A retired car has no finishing position, like every older season.
        retired = next(r for r in results if r["positionText"] == "R")
        self.assertEqual(retired["position"], updater.NULL)
        # Rows of other races are kept exactly as they were written.
        self.assertIn(self.untouched, (self.dir / "results.csv").read_text().splitlines())
        # Running again with the same API data changes no file at all.
        self.assertEqual(self.run_update()[2], [])

    def test_new_teams_drivers_and_statuses_get_new_ids(self):
        self.run_update()
        teams = {r["constructorRef"]: r for r in self.rows("constructors")}
        self.assertIn("newteam", teams)
        self.assertGreater(int(teams["newteam"]["constructorId"]), int(teams["mclaren"]["constructorId"]))
        # Text columns in constructors.csv are quoted, like the existing rows.
        self.assertIn('"newteam","New Team","Swiss"', (self.dir / "constructors.csv").read_text())
        self.assertIn("Retired", {r["status"] for r in self.rows("status")})
        self.assertEqual(len([r for r in self.rows("drivers") if r["driverRef"].startswith("rookie")]), 9)

    def test_constructor_points_and_standings(self):
        self.run_update()
        team_ids = {r["constructorRef"]: r["constructorId"] for r in self.rows("constructors")}
        points = {r["constructorId"]: float(r["points"]) for r in self.rows("constructor_results")}
        # McLaren: drivers 2, 4, 6, 8, 10 -> 20 + 14 + 8 + 2 + 0 points.
        self.assertEqual(points[team_ids["mclaren"]], 44)
        standings = {r["constructorId"]: r["position"] for r in self.rows("constructor_standings")}
        self.assertEqual(standings[team_ids["newteam"]], "1")


if __name__ == "__main__":
    unittest.main()
