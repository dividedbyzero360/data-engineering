import requests
import os

result = requests.get("https://api.github.com/user", headers={"Authorization": f"Bearer {os.getenv('GITHUB_TOKEN')}"}).json()
print(result)