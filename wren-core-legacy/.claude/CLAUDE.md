# wren-core-legacy

Legacy Java-based query engine. Serves as a v2 fallback when wren-core (v3/Rust) fails to process a query. Built on a Trino SQL parser fork.

## Module Layout

| Module | Purpose |
|---|---|
| `trino-parser/` | Forked Trino SQL parser |
| `wren-base/` | Shared base types and utilities |
| `wren-main/` | Core engine logic — connectors, metadata, SQL processing, validation |
| `wren-server/` | HTTP server exposing the engine |
| `wren-tests/` | Integration tests |

## Dev Commands

```bash
mvn clean install                    # Build all modules
mvn clean install -DskipTests        # Build without tests
mvn test -pl wren-tests              # Run integration tests only
```

## Architecture

ibis-server calls this engine as a fallback when v3 (Rust wren-core) fails:

```
ibis-server → POST to WREN_ENGINE_ENDPOINT (Java server)
  → Trino SQL parser → query planning → execution
  → Response
```

## Notes

- This is a **legacy** module — new features go into wren-core (Rust), not here
- The Java engine is maintained for backward compatibility with v2 query paths
- Maven parent: `io.airlift:airbase:279`
- Java version and build config inherited from airbase parent POM
