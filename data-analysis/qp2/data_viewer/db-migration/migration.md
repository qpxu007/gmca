migrate to postgresql


run as root:

sudo -u postgres psql
CREATE USER qp2user;
CREATE DATABASE user_data WITH OWNER = qp2user;
GRANT ALL PRIVILEGES ON DATABASE user_data TO qp2user;

run as qp2user

psql -d user_data -U qp2user -f postgresql_schema_v2.sql
or (\i postgresql_schema_v2.sql)


python driver:

postgresql+psycopg2://qp2user@localhost/user_data