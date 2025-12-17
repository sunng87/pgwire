pgbench -f pgbench/select.sql -j 5 -c 40 -T 30 -h 127.0.0.1 -p 5433 -U postgres -d postgres --no-vacuum --progress 1
