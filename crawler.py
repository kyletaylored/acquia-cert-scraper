import os
from pprint import pprint
from google.oauth2 import service_account
from main import AcquiaRegistry, BigQuery

# Set environment variables
os.environ['BQ_DATASET_ID'] = 'certifications'
os.environ['BQ_TABLE_ID'] = 'records'
key_path = "./keys.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

test = AcquiaRegistry(4, gm=True)
records = test.get_all_records()
pprint(records)
bq = BigQuery(credentials=credentials)
bq.record(records, 'guid')
