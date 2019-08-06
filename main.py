from pprint import pprint
from bs4 import BeautifulSoup
import pandas as pd
from pandas import DataFrame
import json
import requests


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
    request_args = request.args

    if request_args and 'page' in request_args:
        page = request_args['page']
    else:
        page = 0

    registry = AcquiaRegistry(page)

    return registry.get_records()


class AcquiaRegistry:
    # Setting local TZ because all TCEQ monitors are in Texas.

    url = "https://certification.acquia.com/registry"

    # Define paging.
    def __init__(self, page=0):
        self.page = page

    def remove_attrs(self, soup):
        for tag in soup.findAll(True):
            tag.attrs = None
        return soup

    def get_html(self):
        page = self.page
        # Get integer for bad values
        if not isinstance(page, int):
            page = 0

        # Prepare parameters
        params = {
            'page': page
        }

        return requests.get(self.url, params=params).text

    def get_table(self):
        # Get HTML
        html = BeautifulSoup(self.get_html(), 'html.parser')
        # Get tables
        tables = pd.read_html(html.prettify(), header=0)
        # Get only table
        data = tables[0]
        # Rename header
        data = data.rename(columns={"Name  Sort descending": "Name"})

        return data

    def get_json(self):
        data = self.get_table()
        return data.to_json(orient="records")

    def get_records(self, data=None):
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

    def lchop(self, s, sub):
        pprint(len(sub))
        return s[len(sub):]

    def clean_country(self, record):
        # Eventually get list of countries to fix.
        return record

    def clean_org(self, org):
        # Eventually clean org names
        return org
