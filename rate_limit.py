import time
import requests
import json
url = "https://api.github.com/rate_limit"
response = requests.get(url)
print(json.dumps(response.json(), indent=2))
remaining_requests = response.json()["rate"]["remaining"]
print(f"Remaining requests: {remaining_requests}")

if remaining_requests == 0:
    print("Rate limit exceeded")
    time.sleep(60)
