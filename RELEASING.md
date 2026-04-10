# Releasing

Wren Engine uses [release-please](https://github.com/googleapis/release-please) to automate releases. Each component has its own independent release line with separate versioning, changelogs, and publish pipelines.

## Components

| Component | Package | Tag format | Publish target |
|-----------|---------|------------|----------------|
| ibis-server | `ghcr.io/canner/wren-engine-ibis` | `ibis-server-v0.25.0` | Docker (GHCR) |
| mcp-server | — (bundled in ibis-server image) | `mcp-server-v0.25.0` | Version verify only |
| wren-core-py | `wren-core-py` on PyPI | `wren-core-py-v0.2.0` | PyPI |
| wren | `wren-engine` on PyPI | `wren-v0.3.0` | PyPI |

## How it works

### 1. Write conventional commits

release-please reads commit messages to determine version bumps:

| Commit prefix | Version bump | Example |
|---------------|-------------|---------|
| `fix:` | Patch (0.24.6 → 0.24.7) | `fix(ibis-server): handle null in metadata` |
| `feat:` | Minor (0.24.6 → 0.25.0) | `feat(wren): add memory module` |
| `feat!:` or `BREAKING CHANGE` footer | Major | `feat(wren-core-py)!: new session API` |

> All components have `bump-minor-pre-major` enabled — while on 0.x, breaking changes bump minor instead of jumping to 1.0.0. Use `Release-As` (see below) to override this.

Commits are mapped to components by **file path**. A commit touching `wren/src/...` only affects the `wren` release. Commits touching files outside any component directory are ignored.

Prefixes like `chore:`, `docs:`, `refactor:`, `test:` do **not** trigger a version bump but will appear in the changelog if scoped to a component.

### 2. Merge the Release PR

When conventional commits land on `main`, release-please automatically opens (or updates) a **Release PR** per affected component. The PR contains:

- Version bump in `pyproject.toml` (and `Cargo.toml` / `__init__.py` for components with `extra-files`)
- Updated `CHANGELOG.md`
- Updated `.release-please-manifest.json`

Each component gets its own Release PR (`separate-pull-requests: true`). Review and merge when ready — there is no urgency to merge immediately; the PR accumulates changes until you merge it.

### 3. Publish happens automatically

Merging a Release PR triggers:

- **Git tag** created (e.g., `wren-v0.3.0`)
- **GitHub Release** created with auto-generated release notes
- **Publish workflow** dispatched:
  - ibis-server → multi-arch Docker image pushed to GHCR (tagged `<version>` + `latest`)
  - wren-core-py → multi-platform wheels + sdist published to PyPI
  - wren → sdist + wheel published to PyPI
  - mcp-server → version verification

## RC (pre-release) workflow

For pre-release testing, use the **RC Release** workflow (`rc-release.yml`) via GitHub Actions UI:

1. Go to **Actions → RC Release → Run workflow**
2. Select the component and optionally specify an RC version
3. The workflow:
   - Auto-increments the RC number (e.g., `0.25.0-rc.1`, `0.25.0-rc.2`)
   - Creates a git tag (e.g., `ibis-server-v0.25.0-rc.1`)
   - Creates a GitHub pre-release
   - Publishes the artifact (Docker image without `latest` tag, or PyPI with PEP 440 RC version like `0.25.0rc1`)

### RC vs stable coexistence

- RC tags use `-rc.N` suffix — release-please ignores them
- RC does **not** modify version files on `main`
- PyPI RC versions follow PEP 440 (`0.25.0rc1`) — `pip install` won't pick them up without `--pre`
- Docker RC images are tagged with the RC version only (no `latest`)

## Overriding the version

### Force a specific version

Add a `Release-As` footer to any commit:

```
feat(wren): prepare stable release

Release-As: 1.0.0
```

This overrides the automatic version calculation. release-please will use exactly the version you specify in the next Release PR.

You can also use an empty commit if there are no code changes to make:

```bash
git commit --allow-empty -m "chore(wren): prepare 1.0.0 release

Release-As: 1.0.0"
```

### Force a breaking change (major bump)

```
feat(wren)!: redesign query engine

BREAKING CHANGE: WrenEngine constructor signature changed
```

Note: while on 0.x with `bump-minor-pre-major`, this only bumps minor. Use `Release-As` to jump to 1.0.0.

## Configuration files

| File | Purpose |
|------|---------|
| `release-please-config.json` | Component definitions, release types, extra-files |
| `.release-please-manifest.json` | Current version of each component (updated by release-please) |
| `.github/workflows/release-please.yml` | Orchestrator: runs on push to main, dispatches publish workflows |
| `.github/workflows/rc-release.yml` | Manual RC workflow |
| `.github/workflows/publish-ibis-server.yml` | Docker multi-arch build + push |
| `.github/workflows/publish-wren-core-py.yml` | Multi-platform wheel build + PyPI publish |
| `.github/workflows/publish-wren.yml` | sdist + wheel build + PyPI publish |
| `.github/workflows/publish-mcp-server.yml` | Version verification |

## Continuous builds (unchanged)

The existing `build-image.yml` workflow continues to build SHA-tagged Docker images on every push. This is independent of the release-please flow and is not affected by the migration.
