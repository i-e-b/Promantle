# CD into a temp directory, run this:

C:\cockroach-windows\cockroach.exe demo --disable-demo-license --no-example-database --sql-port 26257 --http-port 8088 --echo-sql

# Then run these commands:

#  \unset errexit
#  CREATE ROLE IF NOT EXISTS unit WITH LOGIN PASSWORD 'test';
#  GRANT root TO unit;
