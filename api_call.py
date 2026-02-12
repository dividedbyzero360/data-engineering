import requests
import requests

result = requests.get("https://api.github.com/repos/DataTalksClub/data-engineering-zoomcamp/events").json()
print(result)