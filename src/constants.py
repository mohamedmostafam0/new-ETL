import os

# API params
PATH_LAST_PROCESSED = "./data/last_processed.json"
MAX_LIMIT = 100
MAX_OFFSET = 10000

# We have three parameters in the URL:
# 1. MAX_LIMIT: the maximum number of records to be returned by the API
# 2. date_de_publication: the date from which we want to get the data
# 3. offset: the index of the first result
URL_API = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/rappelconso0/records?limit={}&where=date_de_publication%20%3E%20'{}'&order_by=date_de_publication%20ASC&offset={}"
URL_API = URL_API.format(MAX_LIMIT, "{}", "{}")

#KAFKA PARAMS
TOPIC = os.genenv('KAFKA_TOPIC')
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BROKER')

# POSTGRES PARAMS
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")  
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_USER}:5432/postgres"


