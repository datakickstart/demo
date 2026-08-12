#!/usr/bin/env bash
# Deploy the trips_metrics metric view.
#
#   ./deploy.sh <WAREHOUSE_ID> [PROFILE]
#
# Submits the DDL as a JSON-encoded statement to /api/2.0/sql/statements/.
# `aitools statement submit --file` is NOT used: it strips the YAML block's
# leading indentation and the server rejects the definition.
set -euo pipefail

WAREHOUSE_ID="${1:?usage: deploy.sh <WAREHOUSE_ID> [PROFILE]}"
PROFILE="${2:-DEFAULT}"
DDL_FILE="$(cd "$(dirname "$0")" && pwd)/trips_metrics.sql"

REQUEST_FILE=$(mktemp -t trips_metrics_request)
trap 'rm -f "$REQUEST_FILE"' EXIT

WAREHOUSE_ID="$WAREHOUSE_ID" DDL_FILE="$DDL_FILE" python3 - > "$REQUEST_FILE" <<'PY'
import json, os, sys

ddl = "\n".join(
    line for line in open(os.environ["DDL_FILE"]).read().split("\n")
    if not line.startswith("--")
).strip()
json.dump(
    {
        "warehouse_id": os.environ["WAREHOUSE_ID"],
        "statement": ddl,
        "wait_timeout": "30s",
    },
    sys.stdout,
)
PY

databricks api post /api/2.0/sql/statements/ --json "@$REQUEST_FILE" --profile "$PROFILE"
