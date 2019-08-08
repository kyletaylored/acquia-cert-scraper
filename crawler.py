import requests
import time


url = 'https://us-central1-acquia-certifications-api.cloudfunctions.net/results'

for i in range(0, 135):
    params = {
        'page': i,
        'log': 1
    }

    rq = requests.get(url, params)
    print(rq.url)
    time.sleep(2)
