import os
import json
from pprint import pprint
from google.oauth2 import service_account
from main import AcquiaRegistry, BigQuery

# Load JSON for BQ and Google Auth
key_path = "keys.json"
credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

# Get BigQuery data
with open('bq.json', 'r') as f:
    keys = json.load(f)
# Set environment variables
os.environ['BQ_DATASET_ID'] = keys['BQ_DATASET_ID']
os.environ['BQ_WRITE_TABLE'] = keys['BQ_WRITE_TABLE']
os.environ['BQ_READ_TABLE'] = keys['BQ_READ_TABLE']

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
acquia.print_runtime()
# pprint(records)
# res = bq.record(records, 'guid')
# pprint(res)

# Fetch Grand Master records.
acquia.set_gm(True)
records.append(acquia.get_all_records())
# res = bq.record(records, 'guid')
# pprint(res)

with open('acquia.json', 'w') as outfile:
    json.dump(records, outfile)
