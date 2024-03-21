CREATE SCHEMA tpch;
CREATE TABLE tpch.customer AS SELECT * FROM read_parquet('/usr/src/app/data/customer.parquet');
CREATE TABLE tpch.lineitem AS SELECT * FROM read_parquet('/usr/src/app/data/lineitem.parquet');
CREATE TABLE tpch.nation AS SELECT * FROM read_parquet('/usr/src/app/data/nation.parquet');
CREATE TABLE tpch.orders AS SELECT * FROM read_parquet('/usr/src/app/data/orders.parquet');
CREATE TABLE tpch.part AS SELECT * FROM read_parquet('/usr/src/app/data/part.parquet');