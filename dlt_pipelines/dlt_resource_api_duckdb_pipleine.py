import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator
import os
import random
import json

API_TOKEN = os.getenv("GITHUB_TOKEN")
BASE_URL = "https://api.github.com/repos/DataTalksClub/data-engineering-zoomcamp"
TOPICS = ["data-engineering", "dbt", "docker", "kafka", "kestra", "spark"]
EXAMPLE_EVENT_ID = "6584908513"  # real example: we always inject topics here and write to test_topics.json

# Path for test_topics.json (project root)
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_TOPICS_PATH = os.path.join(_SCRIPT_DIR, "..", f"test_topics_{EXAMPLE_EVENT_ID}.json")


def _inject_topics_into_event(event: dict) -> None:
    """Set payload.pull_request.base.repo.topics to a random subset of TOPICS."""
    payload = event.setdefault("payload", {})
    pr = payload.setdefault("pull_request", {})
    base = pr.setdefault("base", {})
    repo = base.setdefault("repo", {})
    n = random.randint(1, min(4, len(TOPICS)))
    repo["topics"] = random.sample(TOPICS, k=n)


@dlt.resource()
def paginated_getter(GITHUB_TOKEN=dlt.secrets.value):
    client = RESTClient(
        base_url=BASE_URL,
        auth=BearerTokenAuth(token=GITHUB_TOKEN),
        paginator=HeaderLinkPaginator(links_next_key="next"),
    )

    for page in client.paginate("events"):
        if isinstance(page, list):
            for event in page:
                event_id = str(event.get("id", ""))
                if event_id == EXAMPLE_EVENT_ID:
                    _inject_topics_into_event(event)
                    with open(TEST_TOPICS_PATH, "w") as f:
                        json.dump(event, f, indent=2)
                # elif random.random() < 0.25:
                #     _inject_topics_into_event(event)
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

print(pipeline.dataset().events.df())
print(pipeline.dataset().events__payload__pull_request__base__repo__topics.df())