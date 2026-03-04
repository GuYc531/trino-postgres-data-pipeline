import os
import sys
import unittest
from unittest.mock import MagicMock, patch, call
import pandas as pd
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from utils import utils


def make_utils():
    logger = MagicMock()
    ch = MagicMock()
    ch.LAUNCHES_LATEST_URL = 'http://api.spacexdata.com/v5/launches/latest'
    ch.LAUNCHES_HISTORY_URL = 'http://api.spacexdata.com/v5/launches'
    ch.LAUNCHES_TABLE_NAME = 'launches'
    ch.PAYLOADS_TABLE_NAME = 'payloads'
    ch.trino_host = 'trino'
    ch.trino_port = 8080
    ch.trino_user = 'admin'
    ch.trino_catalog = 'postgresql'
    ch.trino_schema = 'public'
    engine = MagicMock()
    return utils(logger=logger, ch=ch, engine=engine)


# ---------------------------------------------------------------------------
# flatten_json
# ---------------------------------------------------------------------------

class TestFlattenJson(unittest.TestCase):

    def setUp(self):
        self.u = make_utils()

    def test_flat_dict(self):
        result = self.u.flatten_json({'a': 1, 'b': 2})
        self.assertEqual(result, {'a': 1, 'b': 2})

    def test_nested_dict(self):
        result = self.u.flatten_json({'a': {'b': 1}})
        self.assertEqual(result, {'a_b': 1})

    def test_list_values(self):
        result = self.u.flatten_json({'items': [10, 20]})
        self.assertEqual(result, {'items_0': 10, 'items_1': 20})

    def test_mixed_nested(self):
        result = self.u.flatten_json({'a': {'b': [1, {'c': 2}]}})
        self.assertEqual(result, {'a_b_0': 1, 'a_b_1_c': 2})

    def test_none_value_preserved(self):
        result = self.u.flatten_json({'a': None})
        self.assertEqual(result, {'a': None})

    def test_empty_dict(self):
        result = self.u.flatten_json({})
        self.assertEqual(result, {})


# ---------------------------------------------------------------------------
# fetch_spacex_data
# ---------------------------------------------------------------------------

class TestFetchSpacexData(unittest.TestCase):

    def setUp(self):
        self.u = make_utils()

    def _mock_response(self, status_code, json_data):
        mock_resp = MagicMock()
        mock_resp.status_code = status_code
        mock_resp.json.return_value = json_data
        return mock_resp

    @patch('utils.requests.get')
    def test_success_with_explicit_url(self, mock_get):
        mock_get.return_value = self._mock_response(200, [{'id': '1'}])
        result = self.u.fetch_spacex_data(url='http://example.com/payloads')
        self.assertEqual(result, [{'id': '1'}])
        mock_get.assert_called_once_with('http://example.com/payloads', timeout=10)

    @patch('utils.requests.get')
    def test_success_latest_uses_latest_url(self, mock_get):
        mock_get.return_value = self._mock_response(200, {'id': 'latest'})
        result = self.u.fetch_spacex_data(latest=True)
        self.assertEqual(result, {'id': 'latest'})
        mock_get.assert_called_once_with(self.u.ch.LAUNCHES_LATEST_URL, timeout=10)

    @patch('utils.requests.get')
    def test_success_history_uses_history_url(self, mock_get):
        mock_get.return_value = self._mock_response(200, [])
        self.u.fetch_spacex_data(latest=False)
        mock_get.assert_called_once_with(self.u.ch.LAUNCHES_HISTORY_URL, timeout=10)

    @patch('utils.requests.get')
    def test_non_200_raises_exception(self, mock_get):
        mock_get.return_value = self._mock_response(404, {})
        with self.assertRaises(Exception) as ctx:
            self.u.fetch_spacex_data(url='http://example.com')
        self.assertIn('404', str(ctx.exception))

    @patch('utils.requests.get')
    def test_timeout_raises_and_logs(self, mock_get):
        mock_get.side_effect = requests.exceptions.Timeout()
        with self.assertRaises(requests.exceptions.Timeout):
            self.u.fetch_spacex_data(url='http://example.com')
        self.u.logger.error.assert_called_once()

    @patch('utils.requests.get')
    def test_network_error_raises_and_logs(self, mock_get):
        mock_get.side_effect = requests.exceptions.ConnectionError()
        with self.assertRaises(requests.exceptions.ConnectionError):
            self.u.fetch_spacex_data(url='http://example.com')
        self.u.logger.error.assert_called_once()

    @patch('utils.requests.get')
    def test_does_not_store_self_data(self, mock_get):
        mock_get.return_value = self._mock_response(200, {'id': 'x'})
        self.u.fetch_spacex_data(url='http://example.com')
        self.assertFalse(hasattr(self.u, 'data'))


# ---------------------------------------------------------------------------
# insert_df_to_db
# ---------------------------------------------------------------------------

class TestInsertDfToDb(unittest.TestCase):

    def setUp(self):
        self.u = make_utils()

    def test_does_not_mutate_caller_dataframe(self):
        df = pd.DataFrame({'col1': [1, 2]})
        original_cols = list(df.columns)
        with patch.object(pd.DataFrame, 'to_sql'):
            self.u.insert_df_to_db(df=df, table_name='test_table')
        self.assertEqual(list(df.columns), original_cols)
        self.assertNotIn('insert_time', df.columns)

    def test_insert_time_added_to_inserted_copy(self):
        inserted = {}

        def capture(self_df, name, con, if_exists, index, chunksize):
            inserted['df'] = self_df.copy()

        with patch.object(pd.DataFrame, 'to_sql', capture):
            df = pd.DataFrame({'col1': [1]})
            self.u.insert_df_to_db(df=df, table_name='t')

        self.assertIn('insert_time', inserted['df'].columns)
        self.assertNotIn('insert_time', df.columns)

    def test_to_sql_called_with_correct_params(self):
        with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
            df = pd.DataFrame({'col1': [1]})
            self.u.insert_df_to_db(df=df, table_name='my_table', batch_size=100)
            mock_to_sql.assert_called_once()
            _, kwargs = mock_to_sql.call_args
            self.assertEqual(kwargs['name'], 'my_table')
            self.assertEqual(kwargs['if_exists'], 'append')
            self.assertFalse(kwargs['index'])
            self.assertEqual(kwargs['chunksize'], 100)

    def test_logs_error_on_to_sql_failure(self):
        with patch.object(pd.DataFrame, 'to_sql', side_effect=Exception("DB error")):
            df = pd.DataFrame({'col1': [1]})
            self.u.insert_df_to_db(df=df, table_name='t')
        self.u.logger.error.assert_called_once()


# ---------------------------------------------------------------------------
# insert_batch_data_to_selected_table
# ---------------------------------------------------------------------------


class TestInsertBatchData(unittest.TestCase):

    def setUp(self):
        self.u = make_utils()

    def test_window_key_renamed_to_window_col(self):
        captured = {}

        def fake_insert(df, table_name, **kwargs):
            captured['df'] = df.copy()

        self.u.insert_df_to_db = fake_insert
        data = [{'id': '1', 'name': 'launch', 'window': 3600}]
        self.u.insert_batch_data_to_selected_table(data=data, table_name='launches')

        self.assertIn('window_col', captured['df'].columns)
        self.assertNotIn('window', captured['df'].columns)

    def test_no_window_key_unchanged(self):
        captured = {}

        def fake_insert(df, table_name, **kwargs):
            captured['df'] = df.copy()

        self.u.insert_df_to_db = fake_insert
        data = [{'id': '1', 'name': 'launch'}]
        self.u.insert_batch_data_to_selected_table(data=data, table_name='launches')

        self.assertNotIn('window', captured['df'].columns)
        self.assertNotIn('window_col', captured['df'].columns)

    def test_all_rows_inserted(self):
        captured = {}

        def fake_insert(df, table_name, **kwargs):
            captured['df'] = df.copy()

        self.u.insert_df_to_db = fake_insert
        data = [{'id': str(i)} for i in range(5)]
        self.u.insert_batch_data_to_selected_table(data=data, table_name='launches')

        self.assertEqual(len(captured['df']), 5)


# ---------------------------------------------------------------------------
# insert_incremental_to_table
# ---------------------------------------------------------------------------

class TestInsertIncrementalToTable(unittest.TestCase):

    def setUp(self):
        self.u = make_utils()

    def test_aligns_to_target_columns(self):
        target_cols = ['id', 'name', 'success', 'insert_time']
        self.u.get_table_columns = MagicMock(return_value=target_cols)

        captured = {}

        def fake_insert(df, table_name, **kwargs):
            captured['df'] = df.copy()

        self.u.insert_df_to_db = fake_insert
        # extra key 'extra_field' should be dropped; 'success' missing → NaN
        data = {'id': 'abc', 'name': 'Falcon 9', 'extra_field': 'ignored'}
        self.u.insert_incremental_to_table(data=data, table_name='launches')

        self.assertListEqual(list(captured['df'].columns), target_cols)
        self.assertEqual(captured['df']['id'].iloc[0], 'abc')

    def test_missing_columns_filled_with_nan(self):
        target_cols = ['id', 'success']
        self.u.get_table_columns = MagicMock(return_value=target_cols)

        captured = {}
        self.u.insert_df_to_db = lambda df, table_name, **kw: captured.update({'df': df.copy()})

        self.u.insert_incremental_to_table(data={'id': '1'}, table_name='launches')
        import numpy as np
        self.assertTrue(pd.isna(captured['df']['success'].iloc[0]))


# ---------------------------------------------------------------------------
# insert_agg_table
# ---------------------------------------------------------------------------

class TestInsertAggTable(unittest.TestCase):

    def setUp(self):
        self.u = make_utils()
        self.u.ch.LAUNCHES_TABLE_NAME = 'launches'
        self.u.ch.PAYLOADS_TABLE_NAME = 'payloads'

    def test_placeholder_substitution(self):
        query_template = 'SELECT * FROM LAUNCHES_TABLE_NAME la JOIN PAYLOADS_TABLE_NAME pa ON la.id = pa.id'
        self.u.load_query = MagicMock(return_value=query_template)

        executed_queries = []

        def fake_read_sql(query, engine):
            executed_queries.append(query)
            return pd.DataFrame({'total_launches': [10]})

        with patch('utils.pd.read_sql', fake_read_sql), \
             patch.object(pd.DataFrame, 'to_sql'):
            self.u.insert_agg_table(table_name='agg_table')

        self.assertIn('launches', executed_queries[0])
        self.assertIn('payloads', executed_queries[0])
        self.assertNotIn('LAUNCHES_TABLE_NAME', executed_queries[0])
        self.assertNotIn('PAYLOADS_TABLE_NAME', executed_queries[0])

    def test_logs_error_when_read_sql_fails(self):
        self.u.load_query = MagicMock(return_value='SELECT 1')

        with patch('utils.pd.read_sql', side_effect=Exception("query failed")):
            self.u.insert_agg_table(table_name='agg_table')

        self.u.logger.error.assert_called_once()

    def test_logs_error_when_to_sql_fails(self):
        self.u.load_query = MagicMock(return_value='SELECT 1')

        with patch('utils.pd.read_sql', return_value=pd.DataFrame({'col': [1]})), \
             patch.object(pd.DataFrame, 'to_sql', side_effect=Exception("insert failed")):
            self.u.insert_agg_table(table_name='agg_table')

        self.u.logger.error.assert_called_once()


# ---------------------------------------------------------------------------
# execute_query_with_trino
# ---------------------------------------------------------------------------

class TestExecuteQueryWithTrino(unittest.TestCase):

    def setUp(self):
        self.u = make_utils()
        self.u.trino_cursor = MagicMock()
        self.u.trino_conn = MagicMock()
        self.u.trino_cursor.fetchall.return_value = [('row1',), ('row2',)]

    def test_returns_rows(self):
        self.u.load_query = MagicMock(return_value='SELECT * FROM LAUNCHES_TABLE_NAME')
        rows = self.u.execute_query_with_trino('exe_1.sql')
        self.assertEqual(rows, [('row1',), ('row2',)])

    def test_substitutes_table_name_placeholders(self):
        self.u.load_query = MagicMock(return_value='SELECT * FROM LAUNCHES_TABLE_NAME JOIN PAYLOADS_TABLE_NAME')
        self.u.execute_query_with_trino('exe_1.sql')
        executed_query = self.u.trino_cursor.execute.call_args[0][0]
        self.assertNotIn('LAUNCHES_TABLE_NAME', executed_query)
        self.assertNotIn('PAYLOADS_TABLE_NAME', executed_query)
        self.assertIn('launches', executed_query)
        self.assertIn('payloads', executed_query)

    def test_cursor_and_connection_closed_on_success(self):
        self.u.load_query = MagicMock(return_value='SELECT 1')
        self.u.execute_query_with_trino('q.sql')
        self.u.trino_cursor.close.assert_called_once()
        self.u.trino_conn.close.assert_called_once()

    def test_cursor_and_connection_closed_on_error(self):
        self.u.load_query = MagicMock(return_value='SELECT 1')
        self.u.trino_cursor.execute.side_effect = Exception("trino error")
        result = self.u.execute_query_with_trino('q.sql')
        self.assertEqual(result, [])
        self.u.trino_cursor.close.assert_called_once()
        self.u.trino_conn.close.assert_called_once()

    def test_logs_error_on_failure(self):
        self.u.load_query = MagicMock(return_value='SELECT 1')
        self.u.trino_cursor.execute.side_effect = Exception("trino error")
        self.u.execute_query_with_trino('q.sql')
        self.u.logger.error.assert_called_once()


if __name__ == '__main__':
    unittest.main()
