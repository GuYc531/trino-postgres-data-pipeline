# trino-postgres-data-pipeline

In this task, I was asked to deploy a docker compose with postgres database and trino as his query engine.
Also to build a pipeline which will insert incremental data and create an aggrigation table.

## assumptions

## setup instructions
to set up the project:
1. clone the repo, main branch
2. create your .env file which should consist a few variables,
   - to look all of them you can look at the config_handler.py (that is where the system set up the environment variables)
3. run the docker compose command
4. make sure the trino container is up and healthy (take more time than the pipeline)
5. running options:
   - to choose between batch insert/ ingestion you should change the anv. variable  'latest' to False/True accordingly
   - to chose which sql file to run over trino you should change the 'trino_query_file_name' to one of the file names in the 'sql/' folder
6. if you want to test the pipeline alone locally you should change the 'POSTGRES_URL' host from 'postgres' -> 'localhost'

## design choices

All the files for pipeline container are in the src folder:
1. data_pipline.py - the main of the pipeline 
2. config_handler.py - handler for environment variables
3. utils.py - handle all the DB connections, helper functions etc.
4. requirements.txt - consists of the required packages to run the pipeline

For the postgres container and the trino container I did not have much of a design choice.
except of defining some variables for the trino in docker/trino/etc/catalog/config.properties file


## tests
### batch insert
I started by filling the postgres with some entities from the API.
for every entity I created 1 table with insert_time column which solves the 'append only' demand.

The mechanism checks whether the entity table exists and if not, fetches the data from the api and creates new table
and inserts the history data.

### ingestion
I ingest new data by flatten it to 1 row dataframe and inserts it to the table of the Launches entity.
function: insert_incremental_to_table in utils creates it

### aggregation
because the aggregation table should be fresh when new data is ingested, I run the query to create it over postgres on every ingest occuring.
I know it is not optimal but based on the business needs, and the data volumes I understand for this task this should be ok
function: insert_agg_table in utils creates it

### further improvements

1. set up a git repo for the sql/ folder to be able to change the queries without update the image
2. id column as different data type ( other than text) for more effective querying
3. get columns names from a repo instead of querying the db table
4. using the schema.md in the api to create the tables instead of manually creating them
5. using the COPY command to insert new data for DB, more effective and robust
6. using pub/sub for the ingestion