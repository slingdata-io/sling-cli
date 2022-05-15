
###### Database to Database ######
# drop / full refresh target table
export PG_DB=postgres://xxxxxxxxxxxxxxxx
export MYSQL_DB=mysql://xxxxxxxxxxxxxxxx

sling run --src-conn PG_DB --src-stream public.transactions --tgt-conn MYSQL_DB  --tgt-object mysql.bank_transactions --mode full-refresh
# OR
sling run -c '
source:
  conn: PG_DB
  stream: public.transactions
target:
  conn: MYSQL_DB
  object: mysql.bank_transactions
  mode: full-refresh
'

# custom sql in-line
export PG_DB=$POSTGRES_URL
export MYSQL_DB=$MYSQL_URL

sling run --src-conn PG_DB --src-stream "select date, description, amount from public.transactions where transaction_type = 'debit'" --tgt-conn MYSQL_DB  --tgt-object mysql.bank_transactions --mode full-refresh
# OR
sling run -c "
source:
  conn: PG_DB
  stream: select date, description, amount from public.transactions where transaction_type = 'debit'
target:
  conn: MYSQL_DB
  object: mysql.bank_transactions
mode: full-refresh
"

# custom sql file
sling run --src-conn PG_DB --src-stream file:///path/to/query.sql --tgt-conn MYSQL_DB  --tgt-object mysql.bank_transactions --mode append
# OR
sling run -c '
source:
  conn: PG_DB
  stream: file:///path/to/query.sql
target:
  conn: MYSQL_DB
  object: mysql.bank_transactions
mode: append
'

# incremental
sling run -c '
source:
  conn: PG_DB
  stream: public.transactions
  update_key: modified_at
  primary_key: id
target:
  conn: MYSQL_DB
  object: mysql.bank_transactions
mode: incremental
'


###### Database to File ######
# CSV export full table

sling run --src-conn PG_DB --src-stream public.transactions --tgt-object file:///tmp/public.transactions.csv
# OR
sling run -c '
source:
  conn: PG_DB
  stream: public.transactions
target:
  object: file:///tmp/public.transactions.csv
'

# CSV dump, custom SQL
sling run -c "
source:
  conn: PG_DB
  stream: select id, created_at, account_id, amount from public.transactions where type = 'A'
target:  
  object: file:///tmp/public.transactions.csv
"

# CSV export full table to S3, gzip
export AWS_ACCESS_KEY_ID=xxxxxxxxxxxxx
export AWS_SECRET_ACCESS_KEY=xxxxxxxxx
sling run --src-conn PG_DB --src-stream public.transactions --tgt-object file:///tmp/public.transactions.csv --tgt-options 'compression: gzip'
# OR
sling run -c '
source:
  conn: PG_DB
  stream: public.transactions
target:  
  object: s3://my-bucket/public.transactions.csv.gz
  options:
    compression: gzip
'

###### File to Database ######
# local CSV import into table
cat /tmp/public.transactions.csv.gz | sling run --tgt-conn PG_DB --tgt-object public.transactions
# OR
sling run --src-stream file:///tmp/public.transactions.csv.gz --tgt-conn PG_DB --tgt-object public.transactions
# OR
sling run -c '
source:
  stream: file:///tmp/public.transactions.csv.gz
target:
  conn: PG_DB
  object: public.transactions
mode: append
'


# CSV folder import into table, incremental
export AWS_ACCESS_KEY_ID=xxxxxxxxxxxxx
export AWS_SECRET_ACCESS_KEY=xxxxxxxxx
sling run -c '
source:
  stream: s3://my-bucket/public.transactions/
  update_key: modified_at
  primary_key: id
target:
  conn: PG_DB
  object: public.transactions
mode: incremental
'