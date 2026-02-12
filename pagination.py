import requests
import os
import json
BASE_URL = "https://api.github.com/repos/DataTalksClub/data-engineering-zoomcamp/events"
headers = {"Authorization": f"Bearer {os.getenv('GITHUB_TOKEN')}"}

page_number = 1

while True:
    response = requests.get(BASE_URL, headers=headers)
    page_data = response.json()

    next_page = response.links.get("next", {}).get("url")

    print(f'Got page {page_number} with {len(page_data)} records')
    print(page_data)

    if not next_page:
        break

    page_number += 1
    BASE_URL = next_page
