# settings.py
import os

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

SCRAPING_DB_DSN = {
    "dbname": 'supply6',
    "user": 'postgres',
    "password": 'root',
    "host":'localhost',
    "port": '5432',
}

PROD_DB_DSN = {
    "dbname": 'test_private2',
    "user": 'postgres',
    "password": 'root',
    "host":'localhost',
    "port": '5432',
}

BATCH_SIZE = 500

