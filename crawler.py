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

# Get records
"""
Crawl Acquia certifications.
"""
acquia = AcquiaRegistry()
bq = BigQuery(credentials=credentials)

# Clear all records
# res = bq.delete_all()
# pprint(res)

# Fetch regular records.
records = acquia.get_all_records()
# res = bq.record(records, 'guid')
# pprint(res)

# Fetch Grand Master records.
acquia.set_gm(True)
records = acquia.get_all_records()
# res = bq.record(records, 'guid')
# pprint(res)

print("Records recorded.")
