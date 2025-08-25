"""Main entry point for the Wren package."""

import code
import json
import sys

from app.model.data_source import DataSource
from app.model.error import ErrorCode, WrenError
from wren import create_session_context


def main():
    """Main entry point for the Wren module."""

    if len(sys.argv) < 2:
        print("Usage: python -m wren <data_source> <mdl_path> [connection_info_path]")  # noqa: T201
        sys.exit(1)

    data_source = sys.argv[1]
    mdl_path = sys.argv[2] if len(sys.argv) > 2 else None
    data_source = DataSource(data_source)

    if len(sys.argv) > 3:
        with open(connection_info_path := sys.argv[3]) as f:
            if not f.readable():
                raise WrenError(
                    ErrorCode.GENERIC_USER_ERROR,
                    f"Cannot read connection info file at {connection_info_path}",
                )

            connection_info = json.load(f)
            # The connection_info file produced by Wren AI dbt integration
            # contains a "type" field to indicate the data source type.
            # If it is present, we need to use the `get_connection_info` method
            # of the DataSource class to get the connection info.
            # Otherwise, we can directly use the connection_info as is.
            if "type" in connection_info:
                connection_info = data_source.get_connection_info(
                    connection_info["properties"],
                )
            else:
                connection_info = data_source.get_connection_info(connection_info)
    else:
        connection_info = None

    session = create_session_context(
        data_source=data_source.name,
        connection_info=connection_info,
        mdl_path=mdl_path,
    )
    print(f"Session created: {session}")  # noqa: T201
    print("You can now interact with the Wren session using the 'wren' variable:")  # noqa: T201
    print("> task = wren.sql('SELECT * FROM your_table').execute()")  # noqa: T201
    print("> print(task.results)")  # noqa: T201
    print("> print(task.formatted_result())")  # noqa: T201
    code.interact(local={"wren": session})


if __name__ == "__main__":
    main()
