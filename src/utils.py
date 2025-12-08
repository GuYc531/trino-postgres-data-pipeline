from pathlib import Path
from typing import Tuple

import pandas as pd
import requests
from sqlalchemy import text


class utils:
    def __init__(self, logger, url, engine, table_name):
        self.logger = logger
        self.url: str = url
        self.flatten_data = None
        self.engine = engine
        self.TABLE_NAME = table_name

    def fetch_latest_spacex_data(self) -> dict:
        try:
            self.logger.info("Fetching latest SpaceX latest launchs")
            response = requests.get(self.url, timeout=10)

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

    def flatten_json(self) -> dict:
        out = {}
        self.logger.info("start to flatten data response")

        def flatten(x, name=''):
            if isinstance(x, dict):
                for a in x:
                    flatten(x[a], f"{name}{a}_")
            elif isinstance(x, list):
                for i, a in enumerate(x):
                    flatten(a, f"{name}{i}_")
            else:
                out[name[:-1]] = x

        flatten(self.data)
        self.logger.info("finish to flatten data response")
        self.flatten_data = out
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

    def get_table_columns(self) -> Tuple[str, str]:
        list_of_columns = [i for i in pd.read_sql(f"SELECT * FROM {self.TABLE_NAME} limit 1;", self.engine).columns.values
                           if i != 'insert_time']
        columns_names = ", ".join(list_of_columns)
        columns_names_for_ingest = ':' + ", :".join(list_of_columns)
        return columns_names, columns_names_for_ingest
