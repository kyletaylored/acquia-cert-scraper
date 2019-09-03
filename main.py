from flask import escape, jsonify, send_file
from flask_csv import send_csv
from pprint import pprint
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse
from hashlib import md5
from google.cloud import bigquery, pubsub_v1
import multiprocessing as mp
import pandas as pd
import json
import csv
import requests
import time
import os
import base64


class BigQuery:
    """
    Create connection to BigQuery instance.
    """

    def __init__(self):
        # Prep BigQuery client
        self.dataset_id = env_vars('BQ_DATASET_ID')
        self.table_id = env_vars('BQ_TABLE_ID')
        self.bq_check = True if self.dataset_id is not None and self.table_id is not None else False
        self.client = None
        self.table = None
        if self.bq_check is True:
            self.client = bigquery.Client()
            table_ref = self.client.dataset(
                self.dataset_id).table(self.table_id)
            self.table = self.client.get_table(table_ref)

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
            self.table, records, row_ids=row_ids)
        pprint(err)

    def query(self, query):
        pprint(query)
        query_job = self.client.query(str(query))

        results = query_job.result()  # Waits for job to complete.
        pprint(results)
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
    default = "https://certification.acquia.com/registry"
    gmaster = "https://certification.acquia.com/registry/grand-masters"

    # Initialize object.
    def __init__(self, page=0, gm=False, log=False):
        # Get integer for bad values
        if not isinstance(page, int):
            page = 0

        self.page = page
        self.time = time.time()
        self.log = log
        self.gm = gm

        # Switch between regular registry and Grand Masters.
        if gm is True:
            self.url = self.gmaster
        else:
            self.url = self.default

    def remove_attrs(self, soup):
        for tag in soup.findAll(True):
            tag.attrs = None
        return soup

    def set_page(self, page):
        self.page = page

    def set_gm(self, gm):
        self.gm = gm

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
            # Clean org
            r["Organization"] = self.clean_org(r["Organization"])

            # Format date
            date = datetime.strptime(r["Awarded"], '%B %d, %Y')
            r["Awarded"] = date.strftime('%Y-%m-%d')

            # Process country
            loc = r["Location"].split(",")
            r["City"] = loc[0].strip()
            r["State"] = loc[1].strip()
            country = self.lchop(r["Location"], loc[0]+", "+loc[1])
            r["Country"] = self.clean_country(country.strip())

            # Process GM differently than standard certs.
            if self.gm is True:
                self.process_gm_record(r)
            else:
                self.process_record(r)

            # Create GUID
            hash_str = r["Name"]+r["Certification"]+r["Location"]
            r["guid"] = self.create_hash(hash_str.encode())

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

    def process_gm_record(self, r):
        # Break down certificate
        name = r["Credential"]
        r["Certification"] = r["Credential"]
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

    def clean_org(self, org):
        # Eventually clean org names
        return org

    def create_hash(self, data):
        hash_str = md5(data)
        return hash_str.hexdigest()


class Pubsub:
    # Pub/Sub vars
    project_id = "acquia-certifications-api"
    topic_name = "crawler-fetch"
    subscription_name = "crawler-run"
    publisher = None
    subscriber = None

    def __init__(self):
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()

    def publish(self, data):

        # The `topic_path` method creates a fully qualified identifier
        # in the form `projects/{project_id}/topics/{topic_name}`
        pid = self.project_id
        tn = self.topic_name
        topic_path = self.publisher.topic_path(pid, tn)

        # Data must be a bytestring
        data = self.encode(data)
        # When you publish a message, the client returns a future.
        future = self.publisher.publish(topic_path, data=data)
        print(future.result())

        print('Published messages.')

    def subscribe(self, data):
        # The `subscription_path` method creates a fully qualified identifier
        # in the form `projects/{project_id}/subscriptions/{subscription_name}`
        subscription_path = self.subscriber.subscription_path(
            self.project_id, self.subscription_name)

        def callback(message):
            print('Received message: {}'.format(message))
            message.ack()

        self.subscriber.subscribe(subscription_path, callback=callback)

        # The subscriber is non-blocking. We must keep the main thread from
        # exiting to allow it to process messages asynchronously in the background.
        print('Listening for messages on {}'.format(subscription_path))
        while True:
            time.sleep(60)

    def encode(self, data):
        data = str(data)
        return data.encode()

    def decode(self, data):
        data = str(data)
        return data.decode()


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
    # limit = escape(request.args.get('limit'))
    fetch = escape(request.args.get('fetch'))
    gm = escape(request.args.get('gm'))
    # Write to BigQuery
    log = True if request.args.get('log') is not None else False

    # Initialize objects.
    registry = AcquiaRegistry(log=log)
    bq = BigQuery()
    pubsub = Pubsub()

    # Run crawler on requst.
    if fetch is not None:
        gm = True if gm is not None else False
        pubsub.publish({"gm": gm})

    # Run record query.
    query = "SELECT * FROM certifications.records AS rec ORDER BY rec.Awarded DESC"
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
    ps = Pubsub()

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
    records = acquia.get_all_records()
    bq.record(records)

    acquia.set_gm(True)
    records = acquia.get_all_records()
    bq.record(records)

    print("Records recorded.")


def env_vars(var):
    # Get environment variables
    return os.environ.get(var, None)


# Local testing
# ps = Pubsub()

# test = AcquiaRegistry(4, gm=True)
# records = test.get_records()
# pprint(records)
# records = test.get_all_records()
# test.convert_to_csv(records)
