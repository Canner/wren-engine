"""Main entry point for the Wren package."""

import code
import json
import sys

from app.model.data_source import DataSource
from wren import create_session_context


def main():
    """Main entry point for the Wren module."""

    if len(sys.argv) < 2:
        print("Usage: python -m wren [data_source] <mdl_path> <connection_info_path>")  # noqa: T201
        sys.exit(1)

    data_source = sys.argv[1]
    mdl_path = sys.argv[2] if len(sys.argv) > 2 else None
    data_source = DataSource(data_source)

    if len(sys.argv) > 3:
        with open(connection_info_path := sys.argv[3]) as f:
            if not f.readable():
                raise ValueError(
                    f"Cannot read connection info file at {connection_info_path}"
                )

            connection_info = json.load(f)
            if "type" in connection_info:
                connection_info = data_source.get_connection_info(
                    connection_info["properties"]
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
    code.interact(local={"wren": session})


if __name__ == "__main__":
    main()
