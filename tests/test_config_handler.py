import os
import sys
import unittest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from config_handler import configHandler

BASE_ENV = {
    'POSTGRES_URL': 'postgresql://admin:admin@localhost:5432/demo',
    'LAUNCHES_LATEST_URL': 'http://api.spacexdata.com/v5/launches/latest',
    'LAUNCHES_HISTORY_URL': 'http://api.spacexdata.com/v5/launches',
    'PAYLOADS_URL': 'http://api.spacexdata.com/v5/payloads',
    'LAUNCHPADS_URL': 'http://api.spacexdata.com/v5/launchpads',
    'LANDPADS_URL': 'http://api.spacexdata.com/v5/landpads',
    'LAUNCHES_TABLE_NAME': 'launches',
    'PAYLOADS_TABLE_NAME': 'payloads',
    'LAUNCHPADS_TABLE_NAME': 'launchpads',
    'LANDPADS_TABLE_NAME': 'landpads',
    'AGG_TABLE_NAME': 'agg_table',
    'trino_host': 'trino',
    'trino_port': '8080',
    'trino_user': 'admin',
    'trino_catalog': 'postgresql',
    'trino_schema': 'public',
    'trino_query_file_name': 'exe_1_Launch_Performance_Over_Time.sql',
}


class TestConfigHandlerLatest(unittest.TestCase):

    def _make_ch(self, latest_value):
        env = {**BASE_ENV, 'latest': latest_value}
        with unittest.mock.patch.dict(os.environ, env, clear=True):
            return configHandler(logger=MagicMock())

    def test_latest_true_lowercase(self):
        ch = self._make_ch('true')
        self.assertTrue(ch.latest)

    def test_latest_true_uppercase(self):
        ch = self._make_ch('True')
        self.assertTrue(ch.latest)

    def test_latest_false_string(self):
        # Core regression: 'False' must NOT evaluate to True
        ch = self._make_ch('False')
        self.assertFalse(ch.latest)

    def test_latest_false_lowercase(self):
        ch = self._make_ch('false')
        self.assertFalse(ch.latest)

    def test_latest_missing_defaults_false(self):
        env = {k: v for k, v in BASE_ENV.items() if k != 'latest'}
        with unittest.mock.patch.dict(os.environ, env, clear=True):
            ch = configHandler(logger=MagicMock())
        self.assertFalse(ch.latest)

    def test_env_vars_loaded(self):
        ch = self._make_ch('false')
        self.assertEqual(ch.POSTGRES_URL, BASE_ENV['POSTGRES_URL'])
        self.assertEqual(ch.trino_port, 8080)
        self.assertEqual(ch.LAUNCHES_TABLE_NAME, 'launches')
