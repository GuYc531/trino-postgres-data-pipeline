import os

class configHandler:
    def __init__(self, logger):
        self.POSTGRES_URL = os.getenv('POSTGRES_URL')
        self.LAUNCHES_LATEST_URL = os.getenv('LAUNCHES_LATEST_URL')
        self.PAYLOADS_URL = os.getenv('PAYLOADS_URL')
        self.LAUNCHPADS_URL = os.getenv('LAUNCHPADS_URL')
        self.LANDPADS_URL = os.getenv('LANDPADS_URL')
        self.LAUNCHES_HISTORY_URL = os.getenv('LAUNCHES_HISTORY_URL')
        self.LAUNCHES_TABLE_NAME = os.getenv('LAUNCHES_TABLE_NAME')
        self.PAYLOADS_TABLE_NAME = os.getenv('PAYLOADS_TABLE_NAME')
        self.LAUNCHPADS_TABLE_NAME = os.getenv('LAUNCHPADS_TABLE_NAME')
        self.LANDPADS_TABLE_NAME = os.getenv('LANDPADS_TABLE_NAME')
        self.AGG_TABLE_NAME = os.getenv('AGG_TABLE_NAME')
        self.trino_host = os.getenv('trino_host')
        self.trino_port = int(os.getenv('trino_port'))
        self.trino_user = os.getenv('trino_user')
        self.trino_catalog = os.getenv('trino_catalog')
        self.trino_schema = os.getenv('trino_schema')
        self.latest = bool(os.getenv('latest'))
        self.trino_query_file_name = os.getenv('trino_query_file_name')
        self.logger = logger
        self.logger.info('initialized environment variables')
