import pyarrow.parquet as pq
import sys

file = pq.ParquetFile(sys.argv[1])

print(f'num_row_groups: {file.num_row_groups}')
print(f'metadata: {file.metadata}')
# print(f'column: {file.metadata.row_group(0).column(3)}')
print(f'compression: {file.metadata.row_group(0).column(0).compression}')
print(file.schema)

data = file.read()
print(data)

for col in data:
  for row in col:
    _ = row