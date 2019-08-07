from flask import escape
from flask import jsonify
from pprint import pprint
from bs4 import BeautifulSoup, SoupStrainer
from urllib.parse import urlparse
import multiprocessing as mp
# from google.cloud import bigquery
import pandas as pd
import json
import requests
import time


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

    if page == 'all':
        registry = AcquiaRegistry(page)
        records = registry.get_all_records()
        return jsonify(records)
    elif page is not None:
        page = page
    else:
        page = 0

    pprint('Page param:' + str(page))

    registry = AcquiaRegistry(page)
    records = registry.get_records()
    return jsonify(records)


class AcquiaRegistry:
    # Static URL
    url = "https://certification.acquia.com/registry"

    # Define paging.
    def __init__(self, page=0):
        # Get integer for bad values
        if not isinstance(page, int):
            page = 0

        self.page = page
        self.time = time.time()

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
        for record in records:
            # Clean org
            record["Organization"] = self.clean_org(record["Organization"])

            # Break down certificate
            certs = record["Certification"].split("-")
            record["Certificate Name"] = str(certs[0]).strip()
            record["Certificate Version"] = str(
                certs[1]).strip() if len(certs) > 1 else ""

            # Process country
            loc = record["Location"].split(",")
            record["City"] = loc[0].strip()
            record["State"] = loc[1].strip()
            country = self.lchop(record["Location"], loc[0]+", "+loc[1])
            record["Country"] = self.clean_country(country.strip())

        return records

    def get_all_records(self):
        # Get all the records.
        page = self.get_last_page()

        # Run processing pool
        pool = mp.Pool(processes=6)
        results = pool.map(self.get_new_record, range(1, page + 1))
        print(results)

        # Request all pages
        return results

    def get_new_record(self, page):
        pprint("Process time: " + str(time.time() - self.time))
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

        return page

    def lchop(self, s, sub):
        return s[len(sub):]

    def clean_country(self, record):
        # Eventually get list of countries to fix.
        return record

    def clean_org(self, org):
        # Eventually clean org names
        return org


# Local testing
# test = AcquiaRegistry(120)
# pprint(test.get_all_records())
