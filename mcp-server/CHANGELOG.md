# Changelog

## [0.25.0](https://github.com/Canner/wren-engine/compare/mcp-server-v0.24.6...mcp-server-v0.25.0) (2026-04-16)


### Features

* Add tool annotations for improved LLM tool understanding ([#1394](https://github.com/Canner/wren-engine/issues/1394)) ([d23ebff](https://github.com/Canner/wren-engine/commit/d23ebff79761aded6e2c731714df47955a2e1a3d))
* **mcp-server:** add version management and release flow ([#1428](https://github.com/Canner/wren-engine/issues/1428)) ([c547eb3](https://github.com/Canner/wren-engine/commit/c547eb3fc8394e604eb77bd97eca81b7dff7b913))
* **mcp-server:** add Web UI for connection info management with read-only mode ([#1447](https://github.com/Canner/wren-engine/issues/1447)) ([24f662e](https://github.com/Canner/wren-engine/commit/24f662ef1563cf5a1ebcef2b16c059b77a7d3fb5))
* **mcp-server:** embed MCP server in Docker image with skills and quickstart guide ([#1425](https://github.com/Canner/wren-engine/issues/1425)) ([1c33247](https://github.com/Canner/wren-engine/commit/1c3324790de03da05cc1c9b9683a894bac4c615b))
* **mcp:** introduce `get_wren_guide` tool for default prompt ([#1360](https://github.com/Canner/wren-engine/issues/1360)) ([ae298f4](https://github.com/Canner/wren-engine/commit/ae298f47cfcaf239b6c68d7531c0f3692708f85d))
* **mcp:** introduce MCP server ([#1094](https://github.com/Canner/wren-engine/issues/1094)) ([a7d985c](https://github.com/Canner/wren-engine/commit/a7d985ce88183a4d696e389c9deb220954abed00))
* **skills:** add auto-update notification and skill versioning ([#1427](https://github.com/Canner/wren-engine/issues/1427)) ([8a57caf](https://github.com/Canner/wren-engine/commit/8a57caf9e976ca1444520333a1c4f35ad0c4c8aa))
* **skills:** add wren-http-api skill, rename generate-mdl, enhance for ClawHub ([#1473](https://github.com/Canner/wren-engine/issues/1473)) ([0843fa2](https://github.com/Canner/wren-engine/commit/0843fa210da9f1997c6f2a0b889041dae06b0754))
* support connectionFilePath and skills overhaul for secrets management ([#1432](https://github.com/Canner/wren-engine/issues/1432)) ([c22c8a6](https://github.com/Canner/wren-engine/commit/c22c8a6923a3d5ade3b503b616feb698ebd9206a))
* **wren:** CLI 0.2.0 — context management, profiles, strict mode & memory ([#1522](https://github.com/Canner/wren-engine/issues/1522)) ([fbec650](https://github.com/Canner/wren-engine/commit/fbec650d4e44a62a3ed7fa3a943c74af83d63402))


### Bug Fixes

* **mcp-server,skills:** fix onboarding blockers from test report ([#1478](https://github.com/Canner/wren-engine/issues/1478)) ([197f2ba](https://github.com/Canner/wren-engine/commit/197f2ba99f34f9047523fbb98346c72e706757c1))
* **mcp-server:** fix DuckDB connection info and Dockerfile caching ([#1461](https://github.com/Canner/wren-engine/issues/1461)) ([a754ded](https://github.com/Canner/wren-engine/commit/a754dedd7b992bb75bbadbf34df2cd8fc943e1aa))


### Performance Improvements

* **mcp-server:** add MDL indexing cache for O(1) lookups ([#1384](https://github.com/Canner/wren-engine/issues/1384)) ([6a397b6](https://github.com/Canner/wren-engine/commit/6a397b66fee978e758a0e578f552f728476bb599))


### Dependencies

* **ibis:** bump cryptography from 46.0.3 to 46.0.5 in /mcp-server ([f5bafe9](https://github.com/Canner/wren-engine/commit/f5bafe9f46d9c1f5d71004f8c5e5f39f645e0aa1))
* **ibis:** bump orjson from 3.10.15 to 3.11.7 in /mcp-server ([59b4b6c](https://github.com/Canner/wren-engine/commit/59b4b6cc7ddb645cc60d9f7931b2921e8c7874a6))
* **ibis:** bump python-multipart from 0.0.20 to 0.0.22 in /mcp-server ([cc42683](https://github.com/Canner/wren-engine/commit/cc426830c9cce8d323663a192da223f6566eef19))
* **mcp-server:** bump PyJWT&gt;=2.12.0 for security patch ([cc31b1f](https://github.com/Canner/wren-engine/commit/cc31b1f66c0b1a461eea8fd1c9444570b594b2ec))


### Documentation

* add per-module .claude/CLAUDE.md and redirect AGENTS.md ([#1466](https://github.com/Canner/wren-engine/issues/1466)) ([12bdc4f](https://github.com/Canner/wren-engine/commit/12bdc4fe9f1b52d41468836e8bb8f11c3e8611d6))
* **skills:** enforce Web UI as sole method for connection info setup ([#1455](https://github.com/Canner/wren-engine/issues/1455)) ([642b7ee](https://github.com/Canner/wren-engine/commit/642b7eead21dfdba03201a614acc32ff35af34d5))
