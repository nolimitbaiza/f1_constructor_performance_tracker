import copy
import unittest
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
