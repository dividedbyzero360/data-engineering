import duckdb
from processing_paginated_data import event_data, process_event_data

processed_events = []
processed_topics = []
for page_data in event_data():
    for event in page_data:
        processed_event, event_topics = process_event_data(event)
        processed_events.append(processed_event)
        processed_topics.extend(event_topics)

#  1. Create a connection to a DuckDB database
conn = duckdb.connect("github_events.db")

current_columns = {row[1] for row in conn.execute("PRAGMA table_info(github_events)").fetchall()}

print(current_columns)

for record in processed_events[10:]:
    for key in record.keys():
        if key not in current_columns:
            col_type = "TEXT"
            if isinstance(record[key], bool):
                col_type = "BOOLEAN"
            elif isinstance(record[key], int):
                col_type = "BIGINT"
            elif isinstance(record[key], float):
                col_type = "DOUBLE"
            print(f"ALTER TABLE github_events ADD COLUMN {key} {col_type}")
            alter_query = f"ALTER TABLE github_events ADD COLUMN {key} {col_type}"
            conn.execute(alter_query)
            print(f"Added new column: {key} {col_type}")
            current_columns.add(key)

columns = sorted(current_columns)

flattened_data = [
    tuple(record.get(col, None) for col in columns)
    for record in processed_events
]

# print(len(flattened_data))
# print(flattened_data)

# 5. Construct dynamic SQL for insertion
placeholders = ", ".join(["?" for _ in columns])
columns_str = ", ".join(columns)


print(placeholders)
print(columns_str)


# 5. Construct dynamic SQL for insertion
placeholders = ", ".join(["?" for _ in columns])
columns_str = ", ".join(columns)

insert_query = f"""
INSERT INTO github_events ({columns_str})
VALUES ({placeholders})
ON CONFLICT (id) DO UPDATE SET {", ".join(f"{col}=excluded.{col}" for col in columns if col != "id")};
"""

print(insert_query)

conn.executemany(insert_query, flattened_data)
print("Data successfully loaded into DuckDB with schema updates!")

# 7. Query the table
df = conn.execute("""SELECT * FROM github_events""").df()

conn.close()

print("\nGitHub Events Data:")

print(df)