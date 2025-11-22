#!/usr/bin/env python3
"""
Generate a Parquet file with very large string values to test DuckDB scanner handling.
This reproduces issue #668 where DuckDB hangs when reading Parquet files with large strings.
"""

import pyarrow as pa
import pyarrow.parquet as pq
import os

def generate_large_string(size=70000):
    """Generate a large string exceeding 65,330 characters."""
    # Create a pattern that's realistic - HTML/JSON-like content
    base_text = """
    <div class="content">
        <h1>This is a test heading with lots of content</h1>
        <p>Lorem ipsum dolor sit amet, consectetur adipiscing elit.
        Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.</p>
        <ul>
            <li>Item 1 with detailed description and metadata</li>
            <li>Item 2 with detailed description and metadata</li>
            <li>Item 3 with detailed description and metadata</li>
        </ul>
        {"metadata": {"tags": ["tag1", "tag2", "tag3"], "attributes": {"color": "red", "size": "large"}}}
    </div>
    """.replace('\n', '')
    # Repeat to exceed 65,330 characters (DuckDB's default string size limit)
    repetitions = (size // len(base_text)) + 1
    return (base_text * repetitions)[:size]

def main():
    # Create output directory
    output_dir = "/tmp/sling-test"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "test_large_strings.parquet")

    # Generate data
    num_rows = 100
    blog_ids = list(range(1, num_rows + 1))

    # Create large strings for each row (all EXACTLY 65,330 chars to match issue #668)
    rich_formats = []
    for i in range(num_rows):
        # All strings exactly 65,330 characters (the threshold where DuckDB may hang)
        size = 65330
        rich_formats.append(generate_large_string(size))

    # Create PyArrow table
    table = pa.table({
        'blog_id': pa.array(blog_ids, type=pa.int64()),
        'rich_format': pa.array(rich_formats, type=pa.string())
    })

    # Write to Parquet file
    pq.write_table(table, output_file)

    # Print statistics
    file_size = os.path.getsize(output_file)
    min_string_len = min(len(s) for s in rich_formats)
    max_string_len = max(len(s) for s in rich_formats)
    avg_string_len = sum(len(s) for s in rich_formats) / len(rich_formats)

    print(f"✓ Parquet file created: {output_file}")
    print(f"  - File size: {file_size / 1024 / 1024:.2f} MB")
    print(f"  - Rows: {num_rows}")
    print(f"  - String lengths: min={min_string_len:,}, max={max_string_len:,}, avg={avg_string_len:,.0f}")
    print(f"  - All strings > 65,330 chars: {min_string_len > 65330}")

if __name__ == "__main__":
    main()
