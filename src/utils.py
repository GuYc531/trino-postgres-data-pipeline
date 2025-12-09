from pathlib import Path
from trino.dbapi import connect

import pandas as pd
import requests
from sqlalchemy import text, inspect
from datetime import datetime


class utils:
    def __init__(self, logger, ch, engine):
        self.logger = logger
        self.engine = engine
        self.ch = ch

    def fetch_spacex_data(self, url: str = None, latest: bool = True):
        try:
            if not url:
                if latest:
                    self.logger.info("Fetching latest SpaceX latest launchs")
                    response = requests.get(self.ch.LAUNCHES_LATEST_URL, timeout=10)
                else:
                    self.logger.info("Fetching history SpaceX latest launchs")
                    response = requests.get(self.ch.LAUNCHES_HISTORY_URL, timeout=10)
            else:
                self.logger.info(f"Fetching history SpaceX from {url.split('/')[-1]}")
                response = requests.get(url, timeout=10)

            if response.status_code != 200:
                raise Exception(f"API responded with status {response.status_code}")

            self.data = response.json()
            self.logger.info("Successfully fetched latest launch.")
            return self.data

        except requests.exceptions.Timeout:
            self.logger.error("Request timed out.")
            raise

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network error: {e}")
            raise

        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            raise

    def flatten_json(self, data) -> dict:
        out = {}

        def flatten(x, name=''):
            if isinstance(x, dict):
                for a in x:
                    flatten(x[a], f"{name}{a}_")
            elif isinstance(x, list):
                for i, a in enumerate(x):
                    flatten(a, f"{name}{i}_")
            else:
                out[name[:-1]] = x

        flatten(data)
        return out

    def execute_query(self, query: str, added_data: dict = None):
        with self.engine.begin() as conn:
            self.logger.info(f"performing query: \n {query}")
            if added_data:
                conn.execute(text(query), added_data)
            else:
                conn.execute(text(query))
            self.logger.info(f"successfully completed query")

    def load_query(self, query_name: str) -> str:
        BASE_DIR = Path(__file__).resolve().parent
        sql_file_path = BASE_DIR.parent / "sql" / f"{query_name}"

        with open(sql_file_path, "r") as f:
            sql = f.read()

        return sql

    def insert_df_to_db(self, df: pd.DataFrame, table_name: str, batch_size=5000) -> None:
        df['insert_time'] = datetime.now()
        try:
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists='append',
                index=False,
                chunksize=batch_size
            )
            self.logger.info(f"Successfully inserted {len(df)} rows using to_sql (chunksize={batch_size}).")
        except Exception as e:
            self.logger.error(f"Error during to_sql: {e}")

    def insert_batch_data_to_selected_table(self, data: list, table_name: str) -> None:
        ls = list()
        for row in data:
            flatten_data = self.flatten_json(data=row)
            if 'window' in flatten_data.keys():
                flatten_data['window_col'] = flatten_data.pop('window')  # saved word in postgres need to be changed
            ls.append(flatten_data)
        flatten_data = pd.DataFrame(ls)
        self.insert_df_to_db(df=flatten_data, table_name=table_name)
        self.logger.info(f"inserted to table {table_name} {len(data)} new rows")

    def get_table_columns(self, table_name: str, schema: str = 'public') -> list:
        try:
            inspector = inspect(self.engine)
            columns_info = inspector.get_columns(table_name, schema=schema)
            column_names = [col['name'] for col in columns_info]
            return column_names

        except Exception as e:
            self.logger.error(f"Error reflecting table {table_name}: {e}")
            return []

    def insert_incremental_to_table(self, data: dict, table_name: str) -> None:
        target_cols = self.get_table_columns(table_name)
        flatten_data = self.flatten_json(data=data)
        flatten_data_df = pd.DataFrame([flatten_data])
        df_aligned = flatten_data_df.reindex(columns=target_cols)
        df_aligned['insert_time'] = datetime.now()
        try:
            df_aligned.to_sql(
                name=f'{table_name}',
                con=self.engine,
                if_exists='append',
                index=False)
            self.logger.info(f"successfully inserted new row to {table_name}")

        except Exception as e:
            self.logger.error(f"Error inserting to table {table_name}: {e}")

    def insert_agg_table(self, table_name: str) -> None:
        query = self.load_query("aggregate_query.sql")
        adjusted_query = query.replace("LAUNCHES_TABLE_NAME", self.ch.LAUNCHES_TABLE_NAME).replace(
            "PAYLOADS_TABLE_NAME",
            self.ch.PAYLOADS_TABLE_NAME)
        agg_df = pd.read_sql(adjusted_query, self.engine)
        try:
            agg_df.to_sql(
                name=f'{table_name}',
                con=self.engine,
                if_exists='append',
                index=False)
            self.logger.info(f"successfully inserted new row to {table_name}")

        except Exception as e:
            self.logger.error(f"Error inserting to table {table_name}: {e}")

    def create_trino_cursor(self) -> None:
        try:
            self.logger.info("start connecting to trino")
            trino_conn = connect(
                host=self.ch.trino_host,
                port=self.ch.trino_port,
                user=self.ch.trino_user,
                catalog=self.ch.trino_catalog,
                schema=self.ch.trino_schema
            )
            self.logger.info("successfully connected to trino")
            self.trino_cursor = trino_conn.cursor()
        except Exception as e:
            self.logger.error(f"Error connecting to trino: {e}")

    def execute_query_with_trino(self, query_file_name: str):
        try:
            self.logger.info(f"loading query: {query_file_name}")
            query = self.load_query(query_name=query_file_name)
            query = query.replace('LAUNCHES_TABLE_NAME', self.ch.LAUNCHES_TABLE_NAME)
            query = query.replace('PAYLOADS_TABLE_NAME', self.ch.PAYLOADS_TABLE_NAME)
            self.logger.info(f"start running query: \n {query}")
            self.trino_cursor.execute(query)
            self.logger.info(f"finish running query, fetching data")
            rows = self.trino_cursor.fetchall()
            self.logger.info(f"finished fetching data")
            return rows
        except Exception as e:
            self.logger.error(f"Error quring trino with user {self.ch.trino_user} query {query_file_name}: {e}")
            return []