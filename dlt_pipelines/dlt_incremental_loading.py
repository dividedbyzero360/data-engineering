import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator
import os

API_TOKEN = os.getenv("GITHUB_TOKEN")
BASE_URL = "https://api.github.com/repos/DataTalksClub/data-engineering-zoomcamp"


@dlt.resource()
def paginated_getter(
    GITHUB_TOKEN=dlt.secrets.value,
    cursor_date=dlt.sources.incremental(
        "created_at", initial_value="2025-02-25"  # 1 <--- field to track, our timestamp
    ),  # 2 <--- Set a default for the first run
):
    print(f"The last created_at extracted is {cursor_date.last_value}")
    client = RESTClient(
        base_url=BASE_URL,
        auth=BearerTokenAuth(token=GITHUB_TOKEN),
        paginator=HeaderLinkPaginator(links_next_key="next"),
    )

    for page in client.paginate("events"):
        yield page


# Define a dlt pipeline with automatic normalization
pipeline = dlt.pipeline(
    pipeline_name="loading_data_example",
    destination="duckdb",
    dataset_name="events",
)
# Run the pipeline with raw nested data
info = pipeline.run(paginated_getter, table_name="events", write_disposition="append")

# Print the load summary
print(info)
# print all the tables which were created
print(pipeline.dataset().schema.data_table_names())

print(pipeline.last_trace)

# print the event table
pipeline.dataset().events.df()
