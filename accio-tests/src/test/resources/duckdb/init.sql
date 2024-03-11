CREATE SCHEMA tpch;
CREATE TABLE tpch.customer AS SELECT * FROM read_parquet('basePath/customer.parquet');
CREATE TABLE tpch.lineitem AS SELECT * FROM read_parquet('basePath/lineitem.parquet');
CREATE TABLE tpch.nation AS SELECT * FROM read_parquet('basePath/nation.parquet');
CREATE TABLE tpch.orders AS SELECT * FROM read_parquet('basePath/orders.parquet');
CREATE TABLE tpch.part AS SELECT * FROM read_parquet('basePath/part.parquet');