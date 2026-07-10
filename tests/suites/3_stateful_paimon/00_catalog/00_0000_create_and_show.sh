#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

WAREHOUSE_PATH="${PAIMON_WAREHOUSE_PATH:-${TESTS_DATA_DIR}/paimon_warehouse}"

"${CURDIR}"/../../../sqllogictests/scripts/prepare_paimon_fs_data.sh

echo "DROP CATALOG IF EXISTS paimon_fs" | bendsql_connect_root

cat <<EOF | bendsql_connect_root
CREATE CATALOG paimon_fs TYPE = PAIMON CONNECTION = (
    METASTORE = 'filesystem',
    WAREHOUSE = '${WAREHOUSE_PATH}'
);
EOF

echo "===== SHOW CREATE CATALOG paimon_fs ====="
echo "SHOW CREATE CATALOG paimon_fs;" | bendsql_connect_root

echo "===== filesystem catalog databases ====="
cat <<EOF | bendsql_connect_root
USE CATALOG paimon_fs;
SHOW DATABASES;
EOF

echo "===== read-only DDL in paimon catalog session ====="
echo "CREATE TABLE regression.new_t(id int);" | bendsql_connect_root 2>&1 | rg -i "read-only|not supported"
