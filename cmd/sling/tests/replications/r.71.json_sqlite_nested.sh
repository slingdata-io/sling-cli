#!/bin/bash

# Create temp directory for nested JSON test files
mkdir -p /tmp/test_nested_json

# Create data1.json with capital "Id" field
cat > /tmp/test_nested_json/data1.json <<'EOF'
[
  {
    "Id": 1,
    "name": "item1",
    "metadata": {
      "type": "A",
      "value": 100,
      "tags": ["tag1", "tag2"]
    },
    "timestamp": "2024-01-01T10:00:00Z"
  },
  {
    "Id": 2,
    "name": "item2",
    "metadata": {
      "type": "B",
      "value": 200,
      "tags": ["tag3"]
    },
    "timestamp": "2024-01-02T11:00:00Z"
  }
]
EOF

# Create data2.json with capital "Id" field
cat > /tmp/test_nested_json/data2.json <<'EOF'
[
  {
    "Id": 3,
    "name": "item3",
    "metadata": {
      "type": "C",
      "value": 300,
      "tags": ["tag4", "tag5", "tag6"]
    },
    "timestamp": "2024-01-03T12:00:00Z"
  },
  {
    "Id": 4,
    "name": "item4",
    "metadata": {
      "type": "D",
      "value": 400,
      "tags": ["tag7", "tag8"]
    },
    "timestamp": "2024-01-04T13:00:00Z"
  }
]
EOF

echo "Test JSON files created successfully"
