
terminalizer init # ~/.terminalizer/config.yml
sling run --src-conn PG_BIONIC --src-stream crypto.ccompare_price_day --tgt-conn POSTGRES --tgt-object public.ccompare_price_day

cd /tmp
terminalizer record demo
export MY_PG_DB='postgresql://postgres:postgres@bionic:55432/postgres?sslmode=disable'
sling run --src-conn MY_PG_DB --src-stream public.ccompare_price_day --stdout > my_file.csv
gzip my_file.csv
cat my_file.csv.gz | sling run --tgt-conn MY_PG_DB --tgt-object public.my_table --mode full-refresh

mv demo.yml /__/devbox/sling-cli/