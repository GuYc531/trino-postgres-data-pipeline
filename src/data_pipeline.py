from dotenv import load_dotenv
from sqlalchemy import create_engine
import logging
from utils import utils
import pandas as pd
from config_handler import configHandler

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)
logger.info("started pipe")

ch = configHandler(logger=logger)
table_names = [ch.LAUNCHES_TABLE_NAME, ch.PAYLOADS_TABLE_NAME, ch.LAUNCHPADS_TABLE_NAME, ch.LANDPADS_TABLE_NAME]
urls = [ch.LAUNCHES_HISTORY_URL, ch.PAYLOADS_URL, ch.LAUNCHPADS_URL, ch.LANDPADS_URL]

engine = create_engine(ch.POSTGRES_URL)

u = utils(logger=logger, ch=ch, engine=engine)
logger.info("initialized utils object")

if not ch.latest:
    # batch insert
    for (table_name, url) in zip(table_names, urls):
        if table_name not in pd.read_sql("select * from pg_tables", engine)['tablename'].values:
            data = u.fetch_spacex_data(url=url)  # populate the table with history
            u.insert_batch_data_to_selected_table(data=data, table_name=table_name)
        else:
            logger.info(f"table {table_name} is already inserted to the DB ")

else:

    # incremental inserts
    data = u.fetch_spacex_data(latest=True)
    u.insert_incremental_to_table(data=data, table_name=ch.LAUNCHES_TABLE_NAME)

    # aggregate table inserts
    u.insert_agg_table(table_name=ch.AGG_TABLE_NAME)

# start querying with trino
u.create_trino_cursor()
query_file_name = ch.trino_query_file_name
rows = u.execute_query_with_trino(query_file_name)

logger.info("\nAggregated results from Trino:")
for row in rows:
    logger.info(row)
