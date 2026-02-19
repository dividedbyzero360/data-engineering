import dlt

@dlt.resource(write_disposition="replace")
def my_resource():
    yield {
        "id": 1,
        "name": "random name",
        "properties": [
            {
                "name": "customer_age",
                "type": "int",
                "label": "Age",
                "notes": [
                    {
                        "text": "string",
                        "author": "string",
                    }
                ]
            }
        ]
    }

# Define a dlt pipeline with automatic normalization
pipeline = dlt.pipeline(
    pipeline_name="loading_examples",
    destination="duckdb",
    dataset_name="resources",
)
# Run the pipeline with raw nested data
info = pipeline.run(my_resource, table_name="random_resource")
print(pipeline.dataset().schema.data_table_names())