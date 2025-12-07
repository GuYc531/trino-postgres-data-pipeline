import pandas as pd
from sqlalchemy import create_engine, text
from trino.dbapi import connect
import time

print("started pipe")

POSTGRES_URL = "postgresql://admin:admin@postgres:5432/demo"
# POSTGRES_URL = "postgresql://admin:admin@localhost:5432/demo"
engine = create_engine(POSTGRES_URL)


# --------------------------------
# 1. Insert Example Data
# --------------------------------

df = pd.DataFrame({
    "id": [1, 2, 1, 3, 2],
    "score": [10, 20, 30, 40, 50]
})


rows = df.to_dict(orient="records")

query = """
INSERT INTO scores (id, score)
VALUES (:id, :score)
"""
create_query = """
CREATE TABLE IF NOT EXISTS scores  (
    id int,
    score int
) ;"""

with engine.begin() as conn:
    conn.execute(text(create_query))
    conn.execute(text(query), rows)

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
