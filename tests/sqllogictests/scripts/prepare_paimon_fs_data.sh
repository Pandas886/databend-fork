#!/usr/bin/env bash
set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../shell_env.sh

WAREHOUSE="${PAIMON_WAREHOUSE_PATH:-${TESTS_DATA_DIR}/paimon_warehouse}"
export PAIMON_WAREHOUSE="${WAREHOUSE}"
mkdir -p "${WAREHOUSE}"

uv run --project "${CURDIR}" "${CURDIR}/prepare_paimon_fs_data.py"
