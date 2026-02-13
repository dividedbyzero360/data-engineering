import requests
import os
import json
from datetime import datetime

def event_data():
    BASE_URL = "https://api.github.com/repos/DataTalksClub/data-engineering-zoomcamp/events"
    headers = {"Authorization": f"Bearer {os.getenv('GITHUB_TOKEN')}"}

    page_number = 1

    while True:
        response = requests.get(BASE_URL, headers=headers)
        page_data = response.json()
        yield page_data
        next_page = response.links.get("next", {}).get("url")

        # print(f'Got page {page_number} with {len(page_data)} records')
        # print(page_data)

        if not next_page:
            break

        page_number += 1
        BASE_URL = next_page

def process_event_data(event):
    result = {}
    result["id"] = event["id"]
    result["type"] = event["type"]
    result["public"] = event["public"]
    result["created_at"] = datetime.fromisoformat(event["created_at"]).timestamp()
    result["actor__id"] = event["actor"]["id"]
    result["actor__login"] = event["actor"]["login"]
    result["repo__id"] = event["repo"]["id"]
    topics = event.get('payload', {}).get('pull_request', {}).get('base', {}).get('repo', {}).get('topics', [])
    processed_topics = []
    for topic in topics:
        processed_topic = {
            'event_id': event['id'],
            'topic_name': topic
        }
        processed_topics.append(processed_topic)
    return result, processed_topics



if __name__ == "__main__": 
    processed_events = []
    processed_topics = []
    for page_data in event_data():
        for event in page_data:
            processed_event, event_topics = process_event_data(event)
            processed_events.append(processed_event)
            processed_topics.extend(event_topics)