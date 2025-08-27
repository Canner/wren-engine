import base64

import orjson
import pytest
from testcontainers.postgres import PostgresContainer

base_url = "/v2/connector/postgres"

manifest = {
    "catalog": "wrenai",
    "schema": "public",
    "models": [
        {
            "name": "t1",
            "refSql": "select * from (values (1, 2), (2, 3), (3, 3)) as t1(id, many_col)",
            "columns": [
                {"name": "id", "type": "integer"},
                {"name": "many_col", "type": "integer"},
            ],
            "primaryKey": "id",
        },
        {
            "name": "t2",
            "refSql": "select * from (values (1, 2), (2, 3), (3, 3)) as t2(id, many_col)",
            "columns": [
                {"name": "id", "type": "integer"},
                {"name": "many_col", "type": "integer"},
            ],
        },
    ],
    "relationships": [
        {
            "name": "t1_id_t2_id",
            "joinType": "ONE_TO_ONE",
            "models": ["t1", "t2"],
            "condition": "t1.id = t2.id",
        },
        {
            "name": "t1_id_t2_many",
            "joinType": "ONE_TO_MANY",
            "models": ["t1", "t2"],
            "condition": "t1.id = t2.many_col",
        },
        {
            "name": "t1_many_t2_id",
            "joinType": "MANY_TO_ONE",
            "models": ["t1", "t2"],
            "condition": "t1.many_col = t2.id",
        },
        {
            "name": "invalid_t1_many_t2_id",
            "joinType": "ONE_TO_ONE",
            "models": ["t1", "t2"],
            "condition": "t1.many_col = t2.id",
        },
    ],
}


@pytest.fixture(scope="module")
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


@pytest.fixture(scope="module")
def postgres(request) -> PostgresContainer:
    pg = PostgresContainer("postgres:16-alpine").start()
    request.addfinalizer(pg.stop)
    return pg


async def test_validation_relationship(
    client, manifest_str, postgres: PostgresContainer
):
    connection_info = _to_connection_info(postgres)
    response = await client.post(
        url=f"{base_url}/validate/relationship_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"relationshipName": "t1_id_t2_id"},
        },
    )
    assert response.status_code == 204

    response = await client.post(
        url=f"{base_url}/validate/relationship_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"relationshipName": "t1_id_t2_many"},
        },
    )
    assert response.status_code == 204

    response = await client.post(
        url=f"{base_url}/validate/relationship_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"relationshipName": "t1_many_t2_id"},
        },
    )
    assert response.status_code == 204


async def test_validation_relationship_not_found(
    client, manifest_str, postgres: PostgresContainer
):
    connection_info = _to_connection_info(postgres)
    response = await client.post(
        url=f"{base_url}/validate/relationship_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"relationshipName": "not_found"},
        },
    )

    assert response.status_code == 422
    assert response.json()["message"] == "Relationship not_found not found in manifest"

    connection_info = _to_connection_info(postgres)
    response = await client.post(
        url=f"{base_url}/validate/relationship_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {},
        },
    )

    assert response.status_code == 422
    assert response.json()["message"] == "relationshipName is required"


async def test_validation_failure(client, manifest_str, postgres: PostgresContainer):
    connection_info = _to_connection_info(postgres)
    response = await client.post(
        url=f"{base_url}/validate/relationship_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"relationshipName": "invalid_t1_many_t2_id"},
        },
    )

    assert response.status_code == 422
    assert (
        response.json()["message"]
        == "Relationship invalid_t1_many_t2_id is not valid: {'result': 'False', 'is_related': 'True', 'left_table_unique': 'False', 'right_table_unique': 'True'}"
    )


def _to_connection_info(pg: PostgresContainer):
    return {
        "host": pg.get_container_host_ip(),
        "port": pg.get_exposed_port(pg.port),
        "user": pg.username,
        "password": pg.password,
        "database": pg.dbname,
    }
