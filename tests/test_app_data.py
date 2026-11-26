import copy
import unittest
from collections import defaultdict
from scripts.build_app_data import build, validate, ROOT


class DataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = build(ROOT / 'data/raw')

    def test_coverage_and_exclusion(self):
        self.assertEqual(self.data['meta']['through'], '2025-08-31')
        self.assertEqual(self.data['seasons'][-1]['complete'], False)
        excluded = [s for s in self.data['standings'] if s['excluded']]
        self.assertTrue(excluded)
        self.assertTrue(all(s['points'] == 0 for s in excluded))

    def test_invalid_counts_and_duplicates_rejected(self):
        data = copy.deepcopy(self.data)
        data['results'][0]['wins'] = 100
        with self.assertRaises(ValueError):
            validate(data)
        data = copy.deepcopy(self.data)
        data['results'].append(data['results'][0])
        with self.assertRaises(ValueError):
            validate(data)

    def test_rescored_points_use_the_2025_system(self):
        races = {r["id"]: r for r in self.data["races"]}
        totals = defaultdict(float)
        for row in self.data["results"]:
            totals[row["raceId"]] += row["rescoredPoints"]

        def race(season, name):
            return next(
                r["id"]
                for r in races.values()
                if r["season"] == season and r["name"] == name
            )

        for race_id, total in totals.items():
            if "Indianapolis" in races[race_id]["name"]:
                self.assertEqual(total, 0)  # never part of the championship
            else:
                self.assertLessEqual(total, 101 + 36)
        # A full Grand Prix gives 25+18+15+12+10+8+6+4+2+1 = 101 points,
        # and a sprint weekend adds 8+7+6+5+4+3+2+1 = 36.
        self.assertEqual(totals[race(2024, "Bahrain Grand Prix")], 101)
        self.assertEqual(totals[race(2024, "Chinese Grand Prix")], 137)
        # China 2025: both Ferraris were disqualified from the Grand Prix,
        # but Hamilton won the sprint (8) and Leclerc was 5th (4).
        ferrari = next(
            r
            for r in self.data["results"]
            if r["raceId"] == race(2025, "Chinese Grand Prix")
            and r["constructorId"] == 6
        )
        self.assertEqual(ferrari["rescoredPoints"], 12)
