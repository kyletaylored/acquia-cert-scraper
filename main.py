from flask import escape, jsonify, send_file
from flask_csv import send_csv
from pprint import pprint
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse
from hashlib import md5
from google.cloud import bigquery
import multiprocessing as mp
import pandas as pd
import json
import csv
import requests
import time
import os
import base64
import re

class BigQuery:
    """
    Create connection to BigQuery instance.
    """

    def __init__(self, credentials=None):
        # Prep BigQuery client
        self.dataset_id = env_vars('BQ_DATASET_ID')
        self.write_table_id = env_vars('BQ_WRITE_TABLE')
        self.read_table_id = env_vars('BQ_READ_TABLE')
        self.bq_check = True if None not in (
            self.dataset_id, self.write_table_id, self.read_table_id) else False
        self.client = None
        self.write_table = None
        self.read_table = None
        if self.bq_check is True:
            if credentials is None:
                self.client = bigquery.Client()
            else:
                self.client = bigquery.Client(
                    credentials=credentials,
                    project=credentials.project_id,
                )

            # Get dataset reference
            dataset = self.client.dataset(self.dataset_id)
            # Get table references
            wt = dataset.table(self.write_table_id)
            rt = dataset.table(self.read_table_id)
            self.write_table = self.client.get_table(wt)
            self.read_table = self.client.get_table(rt)

    def record(self, records, id):
        """Write records to BigQuery instance.

        Arguments:
            records {list} -- A list of records to cycle through.
            id {string} -- The ID field that is unique.
        """
        # Extract IDs from records
        row_ids = []
        for record in records:
            if record[id]:
                row_ids.append(record[id])

        # Insert rows
        err = self.client.insert_rows_json(
            self.write_table, records, row_ids=row_ids)

        return err

    def query(self, query):
        query_job = self.client.query(query)

        # Run query
        results = query_job.result()  # Waits for job to complete.
        return results

    def convert_row(self, row):
        keys = row.keys()
        values = row.values()
        return dict(zip(keys, values))

    def get_records(self, query):
        results = self.query(query)
        records = []
        for row in results:
            records.append(self.convert_row(row))
        return records


class AcquiaRegistry:
    # Static URL
    defaultUrl = "https://certification.acquia.com/registry"
    gmasterUrl = "https://certification.acquia.com/registry/grand-masters"

    # Initialize object.
    def __init__(self, page=0, gm=False):
        # Get integer for bad values
        if not isinstance(page, int):
            page = 0

        self.page = page
        self.time = time.time()
        self.gm = gm

        # Load org replacements
        orgs = {}
        with open('orgs.json', 'r') as f:
            orgs = json.load(f)

        self.orgs = orgs

        # Switch between regular registry and Grand Masters.
        if gm is True:
            self.url = self.gmasterUrl
        else:
            self.url = self.defaultUrl

    def remove_attrs(self, soup):
        for tag in soup.findAll(True):
            tag.attrs = None
        return soup

    def set_page(self, page):
        self.page = page

    def set_gm(self, gm):
        self.gm = gm
        # Switch between regular registry and Grand Masters.
        self.url = self.gmasterUrl if self.gm is True else self.defaultUrl

    def get_html(self, page=None):

        if page is None:
            page = self.page

        # Prepare parameters
        params = {
            'page': page,
            'exam': 'All',
            'cred': 'All',
            'order': 'field_full_name',
            'sort': 'asc'
        }

        # Run request
        query = requests.get(self.url, params=params)

        return query.text

    def get_table(self):
        # Get HTML
        html = BeautifulSoup(self.get_html(), 'html.parser')
        # Get tables
        tables = pd.read_html(html.prettify(), header=0)

        # Check for empty tables
        if len(tables) == 0:
            return False

        # Get only table
        data = tables[0]
        # Rename header (in steps)
        n = {"Name  Sort descending": "Name"}
        gn = {"Grand Master Name  Sort descending": "Name"}
        data = data.rename(columns=n)
        data = data.rename(columns=gn)

        # Clean up memory
        del html
        del tables

        return data

    def get_json(self):
        data = self.get_table()

        # Check for no tables
        if data is False:
            return False

        return data.to_json(orient="records")

    def get_records(self, data=None):
        # Check for bad records
        if data is False:
            return False

        # Get default data
        if data is None:
            data = self.get_json()

        records = json.loads(data)

        for r in records:

            # Process GM differently than standard certs.
            if self.gm is True:
                self.process_gm_record(r)
            else:
                self.process_record(r)

            # Run through processors.
            self.process_org(r)
            self.process_date(r)
            self.process_location(r)
            self.process_guid(r)

        return records

    def get_all_records(self):
        # Get all the records.
        page = self.get_last_page()

        # Run processing pool
        pool = mp.Pool(processes=3)
        results = pool.imap(self.get_new_record, range(0, page + 1))

        # Close and join processing pool.
        pool.close()
        # pool.join()

        # Wait for jobs to finish.
        while self.multiprocess_status():
            time.sleep(10)
            self.print_runtime()

        # Merge into single array
        records = []
        for res in results:
            for rec in res:
                records.append(rec)

        # Processing finished
        pprint("Processing complete: " + str(len(records)) + ", GM: " + str(self.gm))

        return records

    def get_new_record(self, page=0):
        self.set_page(page)
        record = self.get_records()
        return record

    def get_last_page(self):
        # Get main registry page.
        query = requests.get(self.url)
        # Convert to BS4
        html = BeautifulSoup(query.text, 'html.parser')
        # Find last paging link
        link = html.select_one('li.pager__item--last a')
        # Parse URL for parameters
        url = urlparse(link.attrs['href'])
        params = url.query.split('&')
        # Get page numbers
        page = 0
        for param in params:
            if param.find('page') > -1:
                p = param.split('=')
                page = int(p[1])
                break

        # Clean up memory
        del html
        del query
        del link

        return page

    def get_csv(self, data):
        return send_csv(data, 'acquia-certs.csv', data[0].keys(), cache_timeout=0)

    def lchop(self, s, sub):
        return s[len(sub):]

    def process_gm_record(self, r):
        # Break down certificate
        if 'Credential' in r.keys():
            name = r["Credential"]
            del r["Credential"]
        else:
            pprint(r)
            name = "Grand Master"
        r["Certification"] = name
        r["Certificate_Name"] = "Grand Master"
        r["Certificate_Version"] = "D7" if ("7" in str(name)) else "D8"

    def process_record(self, r):
        # Break down certificate
        certs = r["Certification"].split("-")
        r["Certificate_Name"] = str(certs[0]).strip()
        r["Certificate_Version"] = str(
            certs[1]).strip() if len(certs) > 1 else ""

    def clean_country(self, record):
        # Eventually get list of countries to fix.
        return record

    def process_location(self, r):
        # Process country
        loc = r["Location"].split(",")
        r["City"] = loc[0].strip()
        r["State"] = loc[1].strip()
        country = self.lchop(r["Location"], loc[0]+", "+loc[1])
        r["Country"] = self.clean_country(country.strip())

    def process_org(self, r):
        # Only overwrite org is match exists. Clean whitespace
        org = r["Organization"]

        # Don't process null
        if org is None:
            return 0

        # Create default, cleaned org
        r["Organization"] = r["Organization"].strip()

        # Catch weird empties.
        if len(org) < 2:
            r["Organization"] = ""
            return 0

        # Search for key
        for key in self.orgs.keys():
            if re.search(key, org, re.IGNORECASE):
                r["Organization"] = self.orgs.get(key)
                break

    def process_date(self, r):
        # Dates
        now = datetime.now()
        r["timestamp"] = datetime.timestamp(now)

        # Format date
        if r["Awarded"] is None:
            r["Awarded"] = now.strftime('%B %d, %Y')

        date = datetime.strptime(r["Awarded"], '%B %d, %Y')
        r["Awarded"] = date.strftime('%Y-%m-%d')

    def process_guid(self, r):
        # Create GUID
        hash_str = r["Name"]+r["Certification"]
        r["guid"] = self.create_hash(hash_str.encode())

    def create_hash(self, data):
        hash_str = md5(data)
        return hash_str.hexdigest()

    def print_runtime(self):
        pprint("Process time: " + str(round(time.time() - self.time, 2)))

    def multiprocess_status(self):
            act = mp.active_children()
            if len(act)==0:
                return True
            return False

"""
Main functions start
"""


def results(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <http://flask.pocoo.org/docs/1.0/api/#flask.Request>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>.
    """
    # request_json = request.get_json(silent=True)

    # Initialize objects.
    registry = AcquiaRegistry()
    bq = BigQuery()

    # Run record query.
    query = 'SELECT * FROM ' + bq.dataset_id + '.' + bq.read_table_id
    records = bq.get_records(query)

    # pprint(records)
    pprint(request.args)

    # Format request
    if request.args.get('format') == 'csv':
        return registry.get_csv(records)
    else:
        return jsonify(records)


def crawl_records(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict):  The dictionary with data specific to this type of
         event. The `data` field contains the PubsubMessage message. The
         `attributes` field will contain custom attributes if there are any.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata. The `event_id` field contains the Pub/Sub message ID. The
         `timestamp` field contains the publish time.
    """

    print("""This Function was triggered by messageId {} published at {}
    """.format(context.event_id, context.timestamp))

    if 'data' in event:
        data = base64.b64decode(event['data']).decode()
        print(data)

    print(event)

    """
    Crawl Acquia certifications.
    """
    acquia = AcquiaRegistry()
    bq = BigQuery()

    # Clear all records
    # @todo can't do this yet (or ever) do to streaming input conflicts
    # res = bq.delete_all()
    # pprint(res)

    # Fetch regular records.
    records = acquia.get_all_records()
    res = bq.record(records, 'guid')
    pprint(res)

    # Fetch Grand Master records.
    acquia.set_gm(True)
    records = acquia.get_all_records()
    res = bq.record(records, 'guid')
    pprint(res)

    print("Records recorded.")


def env_vars(var):
    # Get environment variables
    return os.environ.get(var, None)
