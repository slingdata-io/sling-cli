mkdir -p /tmp/sling-test-files

# Create JSON file with camelCase columns
cat > /tmp/sling-test-files/camel_case_data.json << 'EOF'
[
  {"userId": 1, "firstName": "John", "lastName": "Doe", "emailAddress": "john.doe@example.com"},
  {"userId": 2, "firstName": "Jane", "lastName": "Smith", "emailAddress": "jane.smith@example.com"},
  {"userId": 3, "firstName": "Bob", "lastName": "Johnson", "emailAddress": "bob.j@example.com"}
]
EOF
