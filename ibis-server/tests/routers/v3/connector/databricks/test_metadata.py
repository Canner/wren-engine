v2_base_url = "/v2/connector/databricks"


async def test_metadata_list_tables(client, connection_info):
    response = await client.post(
        url=f"{v2_base_url}/metadata/tables",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200

    result = next(filter(lambda x: x["name"] == "workspace.wren.t1", response.json()))
    assert result["name"] == "workspace.wren.t1"
    assert result["primaryKey"] is not None
    assert result["description"] == "This is a table comment"
    assert result["properties"] == {
        "catalog": "workspace",
        "schema": "wren",
        "table": "t1",
        "path": None,
    }
    assert len(result["columns"]) == 2
    assert result["columns"][0] == {
        "name": "id",
        "nestedColumns": None,
        "type": "INTEGER",
        "notNull": True,
        "description": "This is a primary key",
        "properties": None,
    }


async def test_metadata_list_constraints(client, connection_info):
    response = await client.post(
        url=f"{v2_base_url}/metadata/constraints",
        json={"connectionInfo": connection_info},
    )

    result = next(
        filter(lambda x: x["constraintName"] == "t2_t1_id_t1_id", response.json())
    )
    assert result["constraintName"] == "t2_t1_id_t1_id"
    assert result["constraintType"] == "FOREIGN KEY"
    assert result["constraintTable"] == "workspace.wren.t2"
    assert result["constraintColumn"] == "t1_id"
    assert result["constraintedTable"] == "workspace.wren.t1"
    assert result["constraintedColumn"] == "id"

    assert response.status_code == 200


async def test_metadata_db_version(client, connection_info):
    response = await client.post(
        url=f"{v2_base_url}/metadata/version",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200
    assert response.text is not None
