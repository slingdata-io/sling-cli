
###### Database to Database ######
# drop target table
export PG_DB_URL=postgres://xxxxxxxxxxxxxxxx
export MYSQL_DB_URL=mysql://xxxxxxxxxxxxxxxx

sling run --src-conn PG_DB_URL --src-stream public.transactions --tgt-conn MYSQL_DB_URL  --tgt-object mysql.bank_transactions --mode drop
# OR
sling run -c '
source:
  conn: PG_DB_URL
  stream: public.transactions
target:
  conn: MYSQL_DB_URL
  object: mysql.bank_transactions
  mode: drop
'

# custom sql in-line
export PG_DB_URL=$POSTGRES_URL
export MYSQL_DB_URL=$MYSQL_URL

sling run --src-conn PG_DB_URL --src-stream "select date, description, amount from public.transactions where transaction_type = 'debit'" --tgt-conn MYSQL_DB_URL  --tgt-object mysql.bank_transactions --mode drop
# OR
sling run -c "
source:
  conn: PG_DB_URL
  stream: select date, description, amount from public.transactions where transaction_type = 'debit'
target:
  conn: MYSQL_DB_URL
  object: mysql.bank_transactions
  mode: drop
"

# custom sql file
sling run --src-conn PG_DB_URL --src-stream file:///path/to/query.sql --tgt-conn MYSQL_DB_URL  --tgt-object mysql.bank_transactions --mode append
# OR
sling run -c '
source:
  conn: PG_DB_URL
  stream: file:///path/to/query.sql
target:
  conn: MYSQL_DB_URL
  object: mysql.bank_transactions
  mode: append
'

# upsert
sling run -c '
source:
  conn: PG_DB_URL
  stream: public.transactions
target:
  conn: MYSQL_DB_URL
  object: mysql.bank_transactions
  mode: upsert
  update_key: modified_at
  primary_key: id
'


###### Database to File ######
# CSV export full table

sling run --src-conn PG_DB_URL --src-stream public.transactions --tgt-object file:///tmp/public.transactions.csv
# OR
sling run -c '
source:
  conn: PG_DB_URL
  stream: public.transactions
target:
  object: file:///tmp/public.transactions.csv
'

# CSV dump, custom SQL
sling run -c "
source:
  conn: PG_DB_URL
  stream: select id, created_at, account_id, amount from public.transactions where type = 'A'
target:  
  object: file:///tmp/public.transactions.csv
"

# CSV export full table to S3, gzip
export AWS_ACCESS_KEY_ID=xxxxxxxxxxxxx
export AWS_SECRET_ACCESS_KEY=xxxxxxxxx
sling run --src-conn PG_DB_URL --src-stream public.transactions --tgt-object file:///tmp/public.transactions.csv --tgt-options 'compression: gzip'
# OR
sling run -c '
source:
  conn: PG_DB_URL
  stream: public.transactions
target:  
  object: s3://my-bucket/public.transactions.csv.gz
  options:
    compression: gzip
'

###### File to Database ######
# local CSV import into table
cat /tmp/public.transactions.csv.gz | sling run --tgt-conn PG_DB_URL --tgt-object public.transactions
# OR
sling run --src-stream file:///tmp/public.transactions.csv.gz --tgt-conn PG_DB_URL --tgt-object public.transactions
# OR
sling run -c '
source:
  stream: file:///tmp/public.transactions.csv.gz
target:
  conn: PG_DB_URL
  object: public.transactions
  mode: append
'


# CSV folder import into table, upsert
export AWS_ACCESS_KEY_ID=xxxxxxxxxxxxx
export AWS_SECRET_ACCESS_KEY=xxxxxxxxx
sling run -c '
source:
  stream: s3://my-bucket/public.transactions/
target:
  conn: PG_DB_URL
  object: public.transactions
  mode: upsert
  update_key: modified_at
  primary_key: id
'