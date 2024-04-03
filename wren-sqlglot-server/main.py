import logging
import os
import sys

import sqlglot
import uvicorn
from fastapi import FastAPI

from dto import TranspileDTO

if sys.version_info < (3, 10):
    sys.exit('Python < 3.10 is not supported')

logging.basicConfig(level=os.getenv('SQLGLOT_LOG_LEVEL', 'INFO'))
logger = logging.getLogger()

sqlglot.logger.setLevel(os.getenv('SQLGLOT_LOG_LEVEL', 'ERROR'))

# Pre run to avoid deadlock when concurrent loading modules
sqlglot.transpile('SELECT ARRAY[1,2,3][1]', read='trino', write='duckdb')
sqlglot.transpile('SELECT custkey, COUNT(*) AS cnt FROM "Order" GROUP BY 1', read='trino', write='bigquery')


app = FastAPI()


@app.get("/")
def read_root():
    return 'OK'


@app.post("/sqlglot/transpile")
def transpile(dto: TranspileDTO):
    logger.debug(f'TranspiledDTO: {dto}')
    transpiled = sqlglot.transpile(dto.sql, read=dto.read, write=dto.write)[0]
    logger.debug(f'Transpiled: {transpiled}')
    return {'sql': transpiled}


if __name__ == "__main__":
    uvicorn.run(app, host='0.0.0.0', port=int(os.getenv('SQLGLOT_PORT', 8000)))
