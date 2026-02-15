import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator
import os
import json

API_TOKEN = os.getenv("GITHUB_TOKEN")
BASE_URL = "https://api.github.com/repos/DataTalksClub/data-engineering-zoomcamp"


@dlt.resource()
def paginated_getter(GITHUB_TOKEN=dlt.secrets.value):
    client = RESTClient(
        base_url=BASE_URL,
        auth=BearerTokenAuth(token=GITHUB_TOKEN),
        paginator=HeaderLinkPaginator(links_next_key="next"),
    )

    for page in client.paginate("events"):
        yield page


events_data = []
for record in paginated_getter():
    events_data.append(record)

# print(json.dumps(events_data[6], indent=2))


# Define a dlt pipeline with automatic normalization
pipeline = dlt.pipeline(
    pipeline_name="github_data",
    destination="duckdb",
    dataset_name="events",
)
# Run the pipeline with raw nested data
info = pipeline.run(paginated_getter, table_name="events", write_disposition="replace")

# Print the load summary
print(info)
# print all the tables which were created
print(pipeline.dataset().schema.data_table_names())