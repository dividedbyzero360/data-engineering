from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator
import os

API_TOKEN = os.getenv('GITHUB_TOKEN')
BASE_URL="https://api.github.com/repos/DataTalksClub/data-engineering-zoomcamp"

def paginated_getter():
    client = RESTClient(
        base_url = BASE_URL,
        auth = BearerTokenAuth(token=API_TOKEN),
        paginator=HeaderLinkPaginator(links_next_key="next")
    )
    for page in client.paginate("events"):
        yield page

# for page_data in paginated_getter():
#     print(page_data)