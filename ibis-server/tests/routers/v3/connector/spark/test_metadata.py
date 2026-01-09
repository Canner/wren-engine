v2_base_url = "/v2/connector/spark"


async def test_metadata_list_tables(client, connection_info):
    response = await client.post(
        url=f"{v2_base_url}/metadata/tables",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200

    tables = response.json()
    assert len(tables) > 0

    # Find the orders table created in conftest
    result = next(
        filter(lambda x: x["name"] == "spark_catalog.default.orders", tables),
        None,
    )
    assert result is not None
    assert result["name"] == "spark_catalog.default.orders"
    assert result["primaryKey"] == ""
    assert result["properties"] == {
        "catalog": "spark_catalog",
        "path": None,
        "schema": "default",
        "table": "orders",
    }
    assert len(result["columns"]) > 0

    # Check a specific column (o_orderkey)
    orderkey_col = next(
        filter(lambda col: col["name"] == "o_orderkey", result["columns"]), None
    )
    assert orderkey_col is not None
    assert orderkey_col["type"] in ["INTEGER", "INT"]
    assert orderkey_col["nestedColumns"] is None
    assert orderkey_col["properties"] is None


async def test_metadata_list_constraints(client, connection_info):
    response = await client.post(
        url=f"{v2_base_url}/metadata/constraints",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200

    constraints = response.json()

    # Spark Connect with Hive catalog typically doesn't store FK constraints
    if len(constraints) > 0:
        # If constraints are supported, verify structure
        constraint = constraints[0]
        assert "constraintName" in constraint
        assert "constraintType" in constraint
        assert "constraintTable" in constraint
        assert "constraintColumn" in constraint
        assert "constraintedTable" in constraint
        assert "constraintedColumn" in constraint
    else:
        # Empty constraints list is expected for most Spark deployments
        assert constraints == []


async def test_metadata_db_version(client, connection_info):
    response = await client.post(
        url=f"{v2_base_url}/metadata/version",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200
    assert response.text is not None

    # Verify it contains "Spark" in the version string
    version_data = response.json()
    version_str = version_data if isinstance(version_data, str) else str(version_data)
    assert "3.5.7" in version_str.lower()
