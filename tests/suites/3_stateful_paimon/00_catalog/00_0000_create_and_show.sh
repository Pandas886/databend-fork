#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

WAREHOUSE_PATH="${PAIMON_WAREHOUSE_PATH:-${TESTS_DATA_DIR}/paimon_warehouse}"

echo "DROP CATALOG IF EXISTS paimon_fs" | bendsql_connect_root
echo "DROP CATALOG IF EXISTS paimon_rest" | bendsql_connect_root

cat <<EOF | bendsql_connect_root
CREATE CATALOG paimon_fs TYPE = PAIMON CONNECTION = (
    METASTORE = 'filesystem',
    WAREHOUSE = '${WAREHOUSE_PATH}'
);
EOF

cat <<EOF | bendsql_connect_root
CREATE CATALOG paimon_rest TYPE = PAIMON CONNECTION = (
    METASTORE = 'rest',
    URI = 'http://127.0.0.1:9088',
    WAREHOUSE = 's3://paimon-warehouse/'
);
EOF

for catalog in paimon_fs paimon_rest; do
	echo "===== Testing ${catalog} ====="
	echo "SHOW CREATE CATALOG ${catalog};" | bendsql_connect_root
	echo """
USE CATALOG ${catalog};
SHOW DATABASES;
""" | bendsql_connect_root
done

echo "CREATE TABLE ddl_ro.t(a int);" | bendsql_connect_root 2>&1 | grep -i "read-only\|not supported"
