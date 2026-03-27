migrate to postgresql


run as root:

sudo -u postgres psql
CREATE USER dhs;
CREATE DATABASE user_data WITH OWNER = dhs;
GRANT ALL PRIVILEGES ON DATABASE user_data TO dhs;

run as dhs

psql -d user_data -U dhs -f postgresql_schema_v2.sql
or (\i postgresql_schema_v2.sql)


python driver:

postgresql+psycopg2://xxx@bl2upper/user_data