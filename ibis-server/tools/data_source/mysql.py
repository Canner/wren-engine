# This script is used to import data into a MySQL 5.7.44 database using SQLAlchemy and Pandas.
# Following are the steps to run this script:
#   docker pull --platform linux/amd64 mysql:5.7.44
#   docker run --name test-mysql -e MYSQL_ROOT_PASSWORD=my-pwd -e MYSQL_DATABASE={database_name} --platform linux/amd64 -p 3306:3306 -d mysql:5.7.44
# Or newer version
#   docker pull --platform linux/amd64 mysql:latest
#   docker run --name test-mysql -e MYSQL_ROOT_PASSWORD=my-pwd -e MYSQL_DATABASE={database_name} --platform linux/amd64 -p 3306:3306 -d mysql:latest

import argparse
import sqlalchemy
import polars as pl
import json
import os

from dotenv import load_dotenv

# Set up argument parsing
parser = argparse.ArgumentParser(description="import data to mysql")
parser.add_argument("dataset_path", help="Path to the dataset")
parser.add_argument("database_name", help="Name of the database")

args = parser.parse_args()


load_dotenv(override=True)
manifest_json_path = os.getenv("WREN_MANIFEST_JSON_PATH")
print("Manifest JSON Path:", manifest_json_path)


# Read and encode the JSON data
with open(manifest_json_path) as file:
    mdl = json.load(file)

connection_url = f"mysql+pymysql://root:my-pwd@localhost:3306/{args.database_name}"
engine = sqlalchemy.create_engine(connection_url)

for model in mdl["models"]:
    path = f"{args.dataset_path}/{model['name']}.parquet"
    pl.read_parquet(path).write_database(
        table_name=model["tableReference"]["table"],
        connection=connection_url,
        if_table_exists="replace"
    )
