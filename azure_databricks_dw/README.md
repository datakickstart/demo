# Azure Databricks for Data Warehousing
Created: 2026-03-10
Updated: 2026-03-11

## Getting Started

### Prerequisites

- [uv](https://docs.astral.sh/uv/getting-started/installation/) installed
- Python 3.12

### Setup

Clone the repo and install dependencies from the lockfile:

```bash
git clone <repo-url>
cd azure_databricks_dw
uv sync
```

`uv sync` creates a `.venv/` and installs all dependencies at the exact versions recorded in `uv.lock`.

### Running scripts

Without activating the virtual environment:

```bash
uv run python generate_retail_data.py
```

Or activate first and use Python directly:

```bash
source .venv/bin/activate
python generate_retail_data.py
```

### Managing dependencies

```bash
# Add a new dependency
uv add <package>

# Update all dependencies within pyproject.toml constraints
uv lock --upgrade

# Update a single dependency
uv lock --upgrade-package <package>

# Sync environment after any lock changes
uv sync
```

> **Note:** `databricks-connect` is pinned to `==18.0` and must match your Databricks Runtime (DBR) version. Do not upgrade it independently.