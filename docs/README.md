# Wren Engine Documentation

Wren Engine is an open-source semantic engine for AI agents and MCP clients. It translates SQL queries through MDL (Model Definition Language) and executes them against 22+ data sources.

## Getting Started

- [Quick Start](quickstart.md) -- Set up a local semantic layer with the jaffle_shop dataset using the Wren CLI and Claude Code. (~15 minutes)

## Core Concepts

- [Wren Project](wren_project.md) -- Project structure, YAML authoring, and how the CLI compiles models into a deployable manifest.

### MDL Reference

- [Model](mdl/model.md) -- Define semantic entities over physical tables or SQL expressions.
- [Relationship](mdl/relationship.md) -- Declare join paths between models for automatic resolution.
- [View](mdl/view.md) -- Named SQL queries that behave as virtual tables.
