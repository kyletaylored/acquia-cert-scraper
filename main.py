from flask import escape, jsonify, send_file
from flask_csv import send_csv
from pprint import pprint
from bs4 import BeautifulSoup
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


class AcquiaRegistry:
    # Static URL
    url = "https://certification.acquia.com/registry"

    # Define paging.
    def __init__(self, page=0, log=False):
        # Get integer for bad values
        if not isinstance(page, int):
            page = 0

        self.page = page
        self.time = time.time()
        self.client = bigquery.Client()
        self.log = log

        # Prep BigQuery client
        self.dataset_id = env_vars('BQ_DATASET_ID')
        self.table_id = env_vars('BQ_TABLE_ID')
        self.bq_check = True if self.dataset_id is not None and self.table_id is not None else False

    def remove_attrs(self, soup):
        for tag in soup.findAll(True):
            tag.attrs = None
        return soup

    def set_page(self, page):
        self.page = page

    def get_html(self, page=None):

        if page is None:
            page = self.page

        # Prepare parameters
        params = {
            'page': page,
            'exam': 'All'
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
        # Rename header
        data = data.rename(columns={"Name  Sort descending": "Name"})

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
            # Clean org
            r["Organization"] = self.clean_org(r["Organization"])

            # Break down certificate
            certs = r["Certification"].split("-")
            r["Certificate Name"] = str(certs[0]).strip()
            r["Certificate Version"] = str(
                certs[1]).strip() if len(certs) > 1 else ""

            # Process country
            loc = r["Location"].split(",")
            r["City"] = loc[0].strip()
            r["State"] = loc[1].strip()
            country = self.lchop(r["Location"], loc[0]+", "+loc[1])
            r["Country"] = self.clean_country(country.strip())

            # Create GUID
            hash_str = r["Name"]+r["Certification"]+r["Location"]
            r["guid"] = self.create_hash(hash_str.encode())

            # Log to BigQuery
            if self.log is True:
                self.bigquery_store(r)

        return records

    def get_all_records(self):
        # Get all the records.
        page = self.get_last_page()

        # Run processing pool
        pool = mp.Pool(processes=3)
        results = pool.map(self.get_new_record, range(1, page + 1))

        # Merge into single array
        records = []
        for res in results:
            for rec in res:
                records.append(rec)

        return records

    def get_new_record(self, page=0):
        # pprint("Process time: " + str(time.time() - self.time))
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

    def clean_country(self, record):
        # Eventually get list of countries to fix.
        return record

    def clean_org(self, org):
        # Eventually clean org names
        return org

    def create_hash(self, data):
        hash_str = md5(data)
        return hash_str.hexdigest()

    def bigquery_store(self, record):
        row = tuple(record.items())
        # Connect to table
        table_ref = self.client.dataset(self.dataset_id).table(self.table_id)
        table = self.client.get_table(table_ref)
        # Insert rows
        err = self.client.insert_rows(table, row, kwargs={'row_ids': 'guid'})
        assert err == []


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

    # Logic goof
    page = escape(request.args.get('page'))
    # Write to BigQuery
    log = True if request.args.get('log') is not None else False

    registry = AcquiaRegistry(log=log)

    # Get all or 1 page
    if page == 'all':
        records = registry.get_all_records()
    else:
        records = registry.get_new_record(page)

    pprint(request.args)

    # Format request
    if request.args.get('format') == 'csv':
        return registry.get_csv(records)
    else:
        return jsonify(records)


def env_vars(var):
    # Get environment variables
    return os.environ.get(var, None)


# Local testing
# test = AcquiaRegistry(120)
# records = test.get_records()
# records = test.get_all_records()
# test.convert_to_csv(records)
