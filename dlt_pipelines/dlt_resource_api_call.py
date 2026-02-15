import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator
import os

API_TOKEN = os.getenv('GITHUB_TOKEN')
BASE_URL="https://api.github.com/repos/DataTalksClub/data-engineering-zoomcamp"

@dlt.resource()
def paginated_getter(GITHUB_TOKEN=dlt.secrets.value):
    print(GITHUB_TOKEN)
    client = RESTClient(
        base_url = BASE_URL,
        auth = BearerTokenAuth(token=GITHUB_TOKEN),
        paginator=HeaderLinkPaginator(links_next_key="next")
    )

    page_number = 1
    for page in client.paginate("events"):
        if page_number==2:
            break
        page_number+= 1
        yield page

for record in paginated_getter():
    print(record)