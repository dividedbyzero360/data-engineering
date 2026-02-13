import duckdb
from processing_paginated_data import event_data, process_event_data

processed_events = []
processed_topics = []
for event_data in event_data():
    for event in event_data:
        processed_event, processed_topics = process_event_data(event)
        processed_events.append(processed_event)
        processed_topics.extend(processed_topics)

#  1. Create a connection to a DuckDB database
conn = duckdb.connect("github_events.db")

# 2. Create the `github_events` Table
conn.execute("""
    CREATE TABLE IF NOT EXISTS github_events (
        id TEXT PRIMARY KEY,
        type TEXT,
        public BOOLEAN,
        created_at DOUBLE,
        actor__id BIGINT,
        actor__login TEXT
    );
""")

flattened_data = [
    (
        record["id"],
        record["type"],
        record["public"],
        record["created_at"],
        record["actor__id"],
        record["actor__login"]
    )
    for record in processed_events
]

# 3. Insert Data into the `github_events` Table
conn.executemany("""
INSERT INTO github_events (id, type, public, created_at, actor__id, actor__login)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (id) DO NOTHING;
""", flattened_data)

print("Data successfully loaded into DuckDB!")

# Query and Print Data
df = conn.execute("SELECT * FROM github_events").df()

conn.close()

print("\nGitHub Events Data:")
print(df)