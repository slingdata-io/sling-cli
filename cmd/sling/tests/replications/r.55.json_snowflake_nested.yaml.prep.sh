mkdir -p /tmp/sling-test-files

# Create nested1.json with 1200 records (2 columns each)
echo '[' > /tmp/sling-test-files/nested1.json
for i in {1..1200}; do
  if [ $i -gt 1 ]; then
    echo "," >> /tmp/sling-test-files/nested1.json
  fi
  if [ $i -gt 1100 ]; then
    echo -n "{\"id\": $i, \"name\": \"User$i\", \"email\": \"user$(($i))@example.com\"}" >> /tmp/sling-test-files/nested1.json
  else
    echo -n "{\"id\": $i, \"name\": \"User$i\"}" >> /tmp/sling-test-files/nested1.json
  fi
done
echo -e "\n]" >> /tmp/sling-test-files/nested1.json

# Create nested2.json with 500 records (3 columns each)
echo '[' > /tmp/sling-test-files/nested2.json
for i in {1..500}; do
  if [ $i -gt 1 ]; then
    echo "," >> /tmp/sling-test-files/nested2.json
  fi
  value=$((i * 100))
  echo -n "{\"id\": $((1200 + i)), \"email\": \"user$((1200 + i))@example.com\", \"value\": $value}" >> /tmp/sling-test-files/nested2.json
done
echo -e "\n]" >> /tmp/sling-test-files/nested2.json