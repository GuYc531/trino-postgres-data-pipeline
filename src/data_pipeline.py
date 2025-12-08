from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy import create_engine, text, Engine, inspect
from trino.dbapi import connect
import logging
from utils import utils
import os
import pandas as pd

load_dotenv()
POSTGRES_URL = os.getenv('POSTGRES_URL')
LAUNCHES_LATEST_URL = os.getenv('LAUNCHES_LATEST_URL')
PAYLOADS_URL = os.getenv('PAYLOADS_URL')
LAUNCHPADS_URL = os.getenv('LAUNCHPADS_URL')
LANDPADS_URL = os.getenv('LANDPADS_URL')
LAUNCHES_HISTORY_URL = os.getenv('LAUNCHES_HISTORY_URL')
LAUNCHES_TABLE_NAME = os.getenv('LAUNCHES_TABLE_NAME')
PAYLOADS_TABLE_NAME = os.getenv('PAYLOADS_TABLE_NAME')
LAUNCHPADS_TABLE_NAME = os.getenv('LAUNCHPADS_TABLE_NAME')
LANDPADS_TABLE_NAME = os.getenv('LANDPADS_TABLE_NAME')
AGG_TABLE_NAME = os.getenv('AGG_TABLE_NAME')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

logger.info("started pipe")

engine = create_engine(POSTGRES_URL)

u = utils(logger=logger, url=LAUNCHES_LATEST_URL, engine=engine, table_name=LAUNCHES_TABLE_NAME,
          history_url=LAUNCHES_HISTORY_URL, LAUNCHES_TABLE_NAME=LAUNCHES_TABLE_NAME, PAYLOADS_TABLE_NAME=PAYLOADS_TABLE_NAME)
logger.info("initialized utils object")

# with engine.begin() as conn:
#     conn.execute(text(f"drop table {LAUNCHES_TABLE_NAME}"))
#     conn.execute(text(f"drop table {PAYLOADS_TABLE_NAME}"))
#     conn.execute(text(f"drop table {LAUNCHPADS_TABLE_NAME}"))
#     conn.execute(text(f"drop table {LANDPADS_TABLE_NAME}"))

# batch insert
for (table_name, url) in zip([LAUNCHES_TABLE_NAME, PAYLOADS_TABLE_NAME, LAUNCHPADS_TABLE_NAME, LANDPADS_TABLE_NAME],
                             [LAUNCHES_HISTORY_URL, PAYLOADS_URL, LAUNCHPADS_URL, LANDPADS_URL]):
    if table_name not in pd.read_sql("select * from pg_tables", engine)['tablename'].values:
        data = u.fetch_spacex_data(url=url)  # populate the table with history
        u.insert_batch_data_to_selected_table(data=data, table_name=table_name)
    else:
        logger.info(f"table {table_name} is already inserted to the DB ")

# incremental inserts
data = u.fetch_spacex_data(latest=True)
u.insert_incremental_to_table(data=data, table_name=LAUNCHES_TABLE_NAME)

# aggregate table inserts
u.insert_agg_table(table_name=AGG_TABLE_NAME)

logger.info("start connecting to trino")
trino_conn = connect(
    host="trino",
    port=8080,
    user="pipeline",
    catalog="postgres",
    schema="public"
)
logger.info("successfully connected to trino")

cursor = trino_conn.cursor()

query = f"""
SELECT 
    id
    insert_time
FROM {LAUNCHES_TABLE_NAME}
"""

cursor.execute(query)
rows = cursor.fetchall()

print("\nAggregated results from Trino:")
for row in rows:
    print(row)
