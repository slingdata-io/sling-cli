


################## ~/sling.demo.prep.sh
apt update && apt install -y wget
wget https://github.com/slingdata-io/sling-cli/releases/download/v1.0.35/sling_1.0.35_linux_amd64.tar.gz
tar -xf sling_1.0.35_linux_amd64.tar.gz
chmod +x sling
export PATH=$PATH:$PWD

sling conns set clickhouse url='clickhouse://admin:dElta123!@100.110.2.70:9000/default'

# download duckdb
export my_duck='duckdb://./duck.db'
sling conns discover my_duck

mkdir ~/.dbt
echo '
snowflake:
  target: prod
  outputs:
    prod:
      type: snowflake
      account: account
      user: user
      password: password
      role: role
      schema: public
      database: database

bigquery:
  target: dev
  outputs:
    dev:
      type: bigquery
      project: project
      dataset: dataset
      keyfile: /path/to/cred.json

' > ~/.dbt/profiles.yml
###########################
# on ubuntu
docker run -v ~/sling.demo.prep.sh:/root/prep.sh --rm -it --hostname sling ubuntu:focal bash
bash /root/prep.sh
export PS1="\[\033[36m\]\u\[\033[m\]@\[\033[32m\]\h:\[\033[33;1m\]\w\[\033[m\]\$ "
export PATH=$PATH:$PWD
cd

##################

sling conns list

export my_pg='postgresql://postgres:postgres@mpc:55432/postgres?sslmode=disable'

sling conns list

sling conns test my_pg

sling conns discover my_pg --schema sling

sling run --src-conn my_pg --src-stream 'select * from sling.accounts order by 1 desc limit 10' --stdout

sling run --src-conn my_pg --src-stream 'sling.accounts' --tgt-conn clichouse --tgt-object default.accounts --mode full-refresh

export my_duck='duckdb://./duck.db'
sling conns list

sling run --src-conn clichouse --src-stream 'default.accounts' --tgt-conn my_duck --tgt-object default.accounts --mode full-refresh

sling run --src-conn my_pg --src-stream 'select * from sling.accounts where rating > 50' --tgt-conn my_duck --tgt-object main.accounts_prime

sling conns discover my_duck

