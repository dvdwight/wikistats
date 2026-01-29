import json
import time
import uuid
import requests
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from pathlib import Path

# Folder where raw parquet files will be written
RAW_DIR = Path(__file__).resolve().parents[3] / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Wikipedia Recent Changes stream
STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

def stream_events(batch_size=50, timeout=20):
    """
    Connects to the Wikipedia EventStream and yields batches of events.
    Stops after timeout seconds or when enough events are collected.
    """
    session = requests.Session()

    headers = { "User-Agent": "wikistats-ingestion/0.1 (https://github.com/dvdwight/wikistats)" }

    with session.get(STREAM_URL, headers=headers, stream=True) as resp:
        resp.raise_for_status()

        events = []
        start_time = time.time()

        print("Connected to Wikipedia EventStream…")
        
        for line in resp.iter_lines():
            if time.time() - start_time > timeout:
                print("Timeout reached, stopping ingestion.")
                break

            if not line:
                # Empty line separates SSE messages
                continue

            line_str = line.decode("utf-8")

            # Skip SSE metadata lines (event:, id:, etc.)
            if line_str.startswith("event:") or line_str.startswith("id:"):
                continue

            # Extract data from "data: {...}" line
            if line_str.startswith("data:"):
                try:
                    json_str = line_str[5:].strip()  # Remove "data:" prefix
                    event = json.loads(json_str)
                    print(f"Processing event: {event.get('title')} (type: {event.get('type')})")
                    events.append(event)
                except json.JSONDecodeError as e:
                    print(f"Failed to parse JSON: {e}")
                    continue

            if len(events) >= batch_size:
                yield events
                events = []

        # Yield remaining events
        if events:
            yield events


def convert_to_arrow(events):
    """
    Convert a list of Wikipedia events into a PyArrow table.
    """
    rows = []
    print(f"Converting {len(events)} events to Arrow format…")
    for e in events:
        rows.append({
            "timestamp": e.get("timestamp"),
            "user": e.get("user"),
            "title": e.get("title"),
            "comment": e.get("comment"),
            "bot": e.get("bot"),
            "minor": e.get("minor"),
            "server_name": e.get("server_name"),
            "wiki": e.get("wiki"),
            "length_new": e.get("length", {}).get("new"),
            "length_old": e.get("length", {}).get("old"),
        })

    return pa.Table.from_pylist(rows)


def write_parquet(table):
    """
    Writes a PyArrow table to a timestamped Parquet file.
    """
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_id = uuid.uuid4().hex[:8]
    path = RAW_DIR / f"pageviews_{ts}_{file_id}.parquet"
    pq.write_table(table, path)
    return path


def ingest(batch_size=50, timeout=3):
    """
    Main ingestion function.
    """
    print(f"Starting ingestion… writing to {RAW_DIR}")

    for batch in stream_events(batch_size=batch_size, timeout=timeout):
        print(f"Processing batch of {len(batch)} events…")
        table = convert_to_arrow(batch)
        print(f"Converted to Arrow table with {table.num_rows} rows.")
        path = write_parquet(table)

        print(f"Wrote {len(batch)} events → {path}")

    print("Ingestion complete.")


if __name__ == "__main__":
    ingest()
