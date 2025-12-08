from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy import create_engine, text, Engine, inspect
from trino.dbapi import connect
import logging
from utils import utils
import os
import pandas as pd
from config_handler import configHandler
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

ch = configHandler(logger=logger)
table_names= [ch.LAUNCHES_TABLE_NAME, ch.PAYLOADS_TABLE_NAME, ch.LAUNCHPADS_TABLE_NAME, ch.LANDPADS_TABLE_NAME]
urls =[ch.LAUNCHES_HISTORY_URL, ch.PAYLOADS_URL, ch.LAUNCHPADS_URL, ch.LANDPADS_URL]

logger.info("started pipe")

engine = create_engine(ch.POSTGRES_URL)

u = utils(logger=logger, url=ch.LAUNCHES_LATEST_URL, engine=engine, table_name=ch.LAUNCHES_TABLE_NAME,
          history_url=ch.LAUNCHES_HISTORY_URL, LAUNCHES_TABLE_NAME=ch.LAUNCHES_TABLE_NAME, PAYLOADS_TABLE_NAME=ch.PAYLOADS_TABLE_NAME)
logger.info("initialized utils object")

# batch insert
for (table_name, url) in zip(table_names, urls):
    if table_name not in pd.read_sql("select * from pg_tables", engine)['tablename'].values:
        data = u.fetch_spacex_data(url=url)  # populate the table with history
        u.insert_batch_data_to_selected_table(data=data, table_name=table_name)
    else:
        logger.info(f"table {table_name} is already inserted to the DB ")

# incremental inserts
data = u.fetch_spacex_data(latest=True)
u.insert_incremental_to_table(data=data, table_name=ch.LAUNCHES_TABLE_NAME)

# aggregate table inserts
u.insert_agg_table(table_name=ch.AGG_TABLE_NAME)

logger.info( "start connecting to trino")
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
with total_payload as (
select la.id,
la.name,
la.launchpad,
 coalesce(pa.mass_kg, 0) + coalesce(pa2.mass_kg,0) as total_mass_kg
from {ch.LAUNCHES_TABLE_NAME} la
left join {ch.PAYLOADS_TABLE_NAME} pa
on pa.id = la.payloads_0
left join {ch.PAYLOADS_TABLE_NAME} pa2
on pa2.id = la.payloads_1)
select launchpad,
count(id) as total_launches,
 AVG(total_mass_kg) as average_payload
from total_payload 
group by launchpad
"""

cursor.execute(query)
rows = cursor.fetchall()

print("\nAggregated results from Trino:")
for row in rows:
    print(row)
