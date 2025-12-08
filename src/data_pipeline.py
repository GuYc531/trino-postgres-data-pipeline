from sqlalchemy import create_engine
from trino.dbapi import connect
import logging
from utils import utils

POSTGRES_URL = "postgresql://admin:admin@postgres:5432/demo"
POSTGRES_URL = "postgresql://admin:admin@localhost:5432/demo"
SPACEX_LATEST_URL = "https://api.spacexdata.com/v5/launches/latest"
TABLE_NAME = "spacex_latest"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

logger.info("started pipe")

engine = create_engine(POSTGRES_URL)

u = utils(logger=logger, url=SPACEX_LATEST_URL, engine=engine, table_name=TABLE_NAME, )
logger.info("initialized utils object")

# --------------------------------
# 1. Insert Example Data
# --------------------------------

data = u.fetch_latest_spacex_data()
flatten_data = u.flatten_json()
flatten_data['window_col'] = flatten_data.pop('window')  # saved word in postgres need to be changed

ddl_sql = u.load_query("create_launches_table.sql")
ddl_sql = ddl_sql.replace("table_name", TABLE_NAME)
u.execute_query(query=ddl_sql)

columns_names, columns_names_for_ingest = u.get_table_columns()

insert_query = u.load_query("insert_new_data.sql")
insert_query = insert_query.replace("table_name", TABLE_NAME,
                                    ).replace("columns_names_for_ingest",
                                              columns_names_for_ingest).replace(
    "columns_names", columns_names)

u.execute_query(query=insert_query, added_data=flatten_data)

print("Inserted data into Postgres!")

# --------------------------------
# 2. Query via Trino
# --------------------------------

trino_conn = connect(
    host="trino",
    port=8080,
    user="pipeline",
    catalog="postgres",
    schema="public"
)

cursor = trino_conn.cursor()

query = """
SELECT 
    id,
    SUM(score) AS total_amount,
    COUNT(*) AS num_transactions
FROM scores
GROUP BY id
ORDER BY total_amount DESC
"""

cursor.execute(query)
rows = cursor.fetchall()

print("\nAggregated results from Trino:")
for row in rows:
    print(row)
