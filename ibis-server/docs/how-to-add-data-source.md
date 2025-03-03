# How to Add a New Data Source


## Implementation

We have a file named `data_source.py` located in the `app/model`. This file contains an enum class called `DataSource`.

Your task is to add a new data source to this `DataSource` enum class.
```python
class DataSource(StrEnum):
    postgres = auto()
```
Additionally, you need to add the corresponding data source and DTO mapping in the enum class `DataSourceExtension`.
```python
class DataSourceExtension(Enum):
    postgres = QueryPostgresDTO
```
Create a new DTO (Data Transfer Object) class in the `model` directory. The `model` directory is located at `app/model` and its contents are defined in the `__init__.py` file.
```python 
class QueryPostgresDTO(QueryDTO):
    connection_info: ConnectionUrl | PostgresConnectionInfo = connection_info_field
```
The connection info depends on [ibis](https://ibis-project.org/backends/postgresql#ibis.postgres.connect).
```python
class PostgresConnectionInfo(BaseModel):
    host: SecretStr = Field(examples=["localhost"])
    port: SecretStr = Field(examples=[5432])
    database: SecretStr
    user: SecretStr
    password: SecretStr
```
We use the base model of [Pydantic](https://docs.pydantic.dev/latest/api/base_model/) to support our class definitions.
Pydantic provides a convenient field type called [Secret Types](https://docs.pydantic.dev/2.0/usage/types/secrets/) that can protect the sensitive information.

Add your xxxConnectionInfo to ConnectionInfo
```python
ConnectionInfo = (
    ...
    | PostgresConnectionInfo
    ...
)
```

Return to the `DataSourceExtension` enum class to implement the `get_{data_source}_connection` function.
This function should be specific to your new data source. For example, if you've added a PostgreSQL data source, you might implement a `get_postgres_connection` function.
```python
@staticmethod
def get_postgres_connection(
    info: ConnectionUrl | PostgresConnectionInfo,
) -> BaseBackend:
    if hasattr(info, "connection_url"):
        return ibis.connect(info.connection_url.get_secret_value())
    return ibis.postgres.connect(
        host=info.host.get_secret_value(),
        port=int(info.port.get_secret_value()),
        database=info.database.get_secret_value(),
        user=info.user.get_secret_value(),
        password=info.password.get_secret_value(),
    )
```

## Test

After implementing the new data source, you should add a test case to ensure it's working correctly.

Create a new test file `test_postgres.py` in the `tests/routers/v2/connector` directory.

Set up the basic test structure:
```python
import pytest
from fastapi.testclient import TestClient
from app.main import app

pytestmark = pytest.mark.postgres
client = TestClient(app)
```
We use [pytest](https://github.com/pytest-dev/pytest) as our test framework.
You can learn more about the pytest [marker](https://docs.pytest.org/en/stable/example/markers.html) and [fixtures](https://docs.pytest.org/en/stable/explanation/fixtures.html).

As we use a strict marker strategy in pytest, you need to declare the new marker in the `pyproject.toml` file.
Open the `pyproject.toml` file and locate the `[tool.pytest.ini_options]` section. Add your new marker to the `markers` list:
```toml
[tool.pytest.ini_options]
markers = [
    "postgres: mark a test as a postgres test",
]
```

If the data source has a Docker image available, you can use [testcontainers-python](https://testcontainers-python.readthedocs.io/en/latest/modules/index.html) to simplify your testing setup:
```python
import pytest
from testcontainers.postgres import PostgresContainer

@pytest.fixture(scope="module")
def postgres(request) -> PostgresContainer:
    pg = PostgresContainer("postgres:16-alpine").start()
    request.addfinalizer(pg.stop)
    return pg
```

Execute the following command to run the test cases and ensure your new feature is working correctly:
```shell
poetry run pytest -m postgres
```
This command runs tests marked with the `postgres` marker.


## Submitting Your Work

After confirming that all tests pass, you can create a Pull Request (PR) to add the new data source to the project.

When creating the PR:
- If you are solving an existing issue, remember to link the PR to that issue.
- If this is a new feature, provide detailed information about the feature in the PR description.

Congratulations! You have successfully added a new data source to the project and created tests to verify its functionality.
