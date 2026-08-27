#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "pyarrow",
# ]
# ///
"""
Generate a small Parquet file for issue #793.
The test loads it once, then re-runs incrementally with no new files.
"""

import os

import pyarrow as pa
import pyarrow.parquet as pq

OUTPUT_DIR = "/tmp/sling-test"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "issue793.parquet")

os.makedirs(OUTPUT_DIR, exist_ok=True)

table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
pq.write_table(table, OUTPUT_PATH)

print(f"Wrote {OUTPUT_PATH} with {table.num_rows} rows")
