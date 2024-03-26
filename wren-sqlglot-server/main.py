#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import sys

import sqlglot
import uvicorn
from fastapi import FastAPI

from dto import TranspileDTO

if sys.version_info < (3, 10):
    sys.exit('Python < 3.10 is not supported')


app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/ready")
def read_root():
    return "OK"


@app.post("/sqlglot/transpile")
def transpile(dto: TranspileDTO):
    print(dto)
    transpiled = sqlglot.transpile(dto.sql, read=dto.read, write=dto.write)[0]
    print('Transpiled:', transpiled)
    return transpiled


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
