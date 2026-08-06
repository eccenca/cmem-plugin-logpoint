# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **CMEM (Corporate Memory) plugin** that retrieves data from a [Logpoint SIEM](https://logpoint.com/) system. The plugin registers a `WorkflowPlugin` subclass (`RetrieveLogs`) with the CMEM framework, enabling users to search logs, define output schemas, and preview available fields/repositories.

## Key Files

- `cmem_plugin_logpoint/search_logs_task.py` — the only source module; contains the `@Plugin`-decorated `RetrieveLogs` class with all plugin logic (search, retrieve, preview actions).
- `cmem_plugin_logpoint/__init__.py` — empty package marker.
- `tests/test_search_logs_task.py` — integration tests against a live Logpoint instance; requires `LOGPOINT_BASE_URL`, `LOGPOINT_ACCOUNT`, `LOGPOINT_SECRET_KEY` env vars (see `tests/conftest.py`).
- `.tasks-plugin.yml` — plugin install/uninstall tasks for cmemc.
- `Taskfile.yaml` — auto-generated from the copier template; see `TaskfileCustom.yml` for project-local overrides.

## Architecture

The plugin follows a single-class pattern: one `RetrieveLogs` class decorated with `@Plugin(...)`, which declares parameters, actions, and documentation inline. The CMEM framework discovers and instantiates it at runtime. Key methods:

- `execute()` — the workflow operator entry point; dispatches to `search_start()` → `search_retrieve_logs()`.
- `preview_output_paths()` / `preview_repositories()` — explorer actions for schema customization.
- `generate_schema()` — builds an `EntitySchema` from user-specified output paths.

All external API calls go to the Logpoint `/getsearchlogs` and `/Repo/get_all_searchable_logpoint` endpoints via `requests`. Authentication uses username/secret-key post parameters (for search) and JWT (for repo listing).

## Development Commands

```bash
# Install deps
task poetry:install

# Run all checks (linters + tests)
task check

# Run a single checker
task check:ruff      # style + lint
task check:mypy      # typing
task check:deptry    # unused/missing deps
task check:trivy     # vulnerability scan
task check:pytest    # unit + integration tests

# Format & fix
task format:fix          # obvious fixes
task format:fix-unsafe   # also unsafe fixes

# Build distribution
task build

# Install plugin into Corporate Memory
task install
```

To run a single test: `poetry run pytest tests/test_search_logs_task.py::test_negative_limit_init -v`

Tests require a live Logpoint instance. Set env vars from `.env` (already gitignored) before running integration tests.

## Conventions

- **Python 3.13** only (see `.python-version`).
- **Poetry** for dependency management; never use pip directly.
- **Ruff** for linting/formatting (line-length 100, target py313); config in `pyproject.toml[tool.ruff]`.
- **pre-commit** hooks mirror the CI checks (ruff, poetry-check/lock, trivy). Run `pre-commit run --all-files` before committing.
- The project is generated from the [eccenca/cmem-plugin-template](https://github.com/eccenca/cmem-plugin-template); `.copier-answers.yml` records template settings.