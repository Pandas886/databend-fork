#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

WAREHOUSE_PATH="${PAIMON_WAREHOUSE_PATH:-${TESTS_DATA_DIR}/paimon_warehouse}"

echo "DROP CATALOG IF EXISTS paimon_fs" | bendsql_connect_root

cat <<EOF | bendsql_connect_root
CREATE CATALOG paimon_fs TYPE = PAIMON CONNECTION = (
    METASTORE = 'filesystem',
    WAREHOUSE = '${WAREHOUSE_PATH}'
);
USE CATALOG paimon_fs;
SELECT id, name FROM regression.append_t ORDER BY id;
SELECT id, name FROM regression.pk_t ORDER BY id;
SELECT id FROM regression.append_t WHERE id = 0 ORDER BY id LIMIT 1;
EOF

echo "CREATE TABLE regression.new_t(id int);" | bendsql_connect_root 2>&1 | grep -i "read-only\|not supported"
