#!/bin/bash

mkdir -p /tmp/sling-test-large-data

# Create CSV file with 1M rows
echo "Creating CSV with 100,000 rows..."

# Write CSV header
cat > /tmp/sling-test-large-data/stock_movement.csv << 'EOF'
id,entity_type,entity_action,entity_id,entity_description,stock_id,stock_location,product_id,website_id,requester_id,qty_expected,qty_processed,qty_outaway,qty_available,qty_allocated,qty_reserved,qty_picked,qty_shipped,qty_decline,qty_decline_description,reserve_id,parent_type,movement_created_at,year
EOF

# Generate 1M rows of data efficiently using awk
awk 'BEGIN {
  split("ORDER TRANSFER ADJUSTMENT RETURN PURCHASE", entity_types)
  split("CREATE UPDATE DELETE ALLOCATE RELEASE", entity_actions)
  split("WH-A WH-B WH-C STORE-1 STORE-2", locations)
  split("SALES_ORDER TRANSFER_ORDER ADJUSTMENT RETURN_ORDER", parent_types)
  
  for (i = 1; i <= 100000; i++) {
    entity_type = entity_types[(i % 5) + 1]
    entity_action = entity_actions[(i % 5) + 1]
    location = locations[(i % 5) + 1]
    parent_type = parent_types[(i % 4) + 1]
    
    product_id = (i % 10000) + 1
    website_id = (i % 5) + 1
    requester_id = (i % 100) + 1
    reserve_id = (i % 50000) + 1
    year = 2020 + (i % 5)
    
    # Generate quantities with 8 decimal places
    qty_expected = sprintf("%d.%08d", i % 1000, i % 10000000)
    qty_processed = sprintf("%d.%08d", i % 900, i % 10000000)
    qty_outaway = sprintf("%d.%08d", i % 100, i % 10000000)
    qty_available = sprintf("%d.%08d", i % 800, i % 10000000)
    qty_allocated = sprintf("%d.%08d", i % 500, i % 10000000)
    qty_reserved = sprintf("%d.%08d", i % 300, i % 10000000)
    qty_picked = sprintf("%d.%08d", i % 400, i % 10000000)
    qty_shipped = sprintf("%d.%08d", i % 350, i % 10000000)
    
    decline = ""
    decline_desc = ""
    if (i % 100 == 0) {
      decline = "5.00000000"
      decline_desc = "Quality issues"
    }
    
    # Generate timestamp
    month = sprintf("%02d", (i % 12) + 1)
    day = sprintf("%02d", (i % 28) + 1)
    hour = sprintf("%02d", i % 24)
    minute = sprintf("%02d", i % 60)
    second = sprintf("%02d", i % 60)
    timestamp = sprintf("%d-%s-%s %s:%s:%s", year, month, day, hour, minute, second)
    
    printf "%d,%s,%s,%d,%s-%d,SKU-%d,%s,%d,%d,%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%s,%s,%d\n",
      i, entity_type, entity_action, i * 10, entity_type, i, product_id, location,
      product_id, website_id, requester_id, qty_expected, qty_processed, qty_outaway,
      qty_available, qty_allocated, qty_reserved, qty_picked, qty_shipped, decline,
      decline_desc, reserve_id, parent_type, timestamp, year
    
    # Progress indicator
    if (i % 100000 == 0) {
      print "  Generated " i " rows..." > "/dev/stderr"
    }
  }
}' >> /tmp/sling-test-large-data/stock_movement.csv

echo "CSV file created with 100,000 rows at /tmp/sling-test-large-data/stock_movement.csv"
