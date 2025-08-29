# This script is used to import data into a PostgreSQL database using SQLAlchemy and Pandas.
# Following are the steps to run this script:
#   docker pull postgres
#   docker run --name test-postgres -e POSTGRES_PASSWORD=my-pwd -e POSTGRES_DB={database_name} -p 5432:5432 -d postgres

import argparse
import sqlalchemy
import polars as pl
import json
import os

from dotenv import load_dotenv

# Set up argument parsing
parser = argparse.ArgumentParser(description="import data to postgres")
parser.add_argument("dataset_path", help="Path to the dataset")
parser.add_argument("database_name", help="Name of the database")

args = parser.parse_args()


load_dotenv(override=True)
manifest_json_path = os.getenv("WREN_MANIFEST_JSON_PATH")
print("Manifest JSON Path:", manifest_json_path)


# Read and encode the JSON data
with open(manifest_json_path) as file:
    mdl = json.load(file)

connection_url = f"postgresql://postgres:my-pwd@localhost:5432/{args.database_name}"
engine = sqlalchemy.create_engine(connection_url)

for model in mdl["models"]:
    try:
        path = f"{args.dataset_path}/{model['name']}.parquet"
        df = pl.read_parquet(path)
        for column in df.columns:
            # polars will input the decimal as text to postgres.
            # It's better to cast a numeric type for the similar behavior.
            if df[column].dtype == pl.Decimal:
                df = df.with_columns(pl.col(column).cast(pl.Float64))
        df.write_database(
            table_name=model["tableReference"]["table"],
            connection=connection_url,
            if_table_exists="replace"
        )
    except Exception as e:
        print(f"Error processing model {model['name']}: {e}")
