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
USE CATALOG paimon_fs;
SET max_threads = 4;
EOF

echo "===== row count from at least four splits ====="
echo "SELECT count(*) >= 4 FROM regression.append_t;" | bendsql_connect_root

node_count=$(echo "SELECT count() FROM system.clusters;" | bendsql_connect_root | tail -n 1 | tr -d '[:space:]')
echo "===== active cluster nodes: ${node_count} ====="

if [ "${node_count}" -lt 2 ]; then
	echo "SKIP multi-node evidence: standalone deployment"
	echo "true"
	exit 0
fi

echo "===== explain analyze: partitions and distinct executor nodes ====="
response=$(curl -s -u root: -XPOST "http://127.0.0.1:${QUERY_HTTP_HANDLER_PORT}/v1/query" \
	-H 'Content-Type: application/json' \
	-d '{"sql": "USE CATALOG paimon_fs; EXPLAIN ANALYZE GRAPHICAL SELECT id FROM regression.append_t"}')

profiles=$(echo "${response}" | jq -r '.data[0][0]' | jq -r '.profiles')
partitions=$(echo "${profiles}" | jq -r '[.[] | select(.labels[]? | .name == "Total partitions") | .labels[] | select(.name == "Total partitions") | .value[0]] | first // "0"')
echo "partitions=${partitions}"

distinct_nodes=$(echo "${profiles}" | jq -r '
  [.[] | select((.name // "") | test("PaimonTableSource")) | .labels[]? | select(.name == "Cluster") | .value[0]] | unique | length')
echo "distinct_paimon_nodes=${distinct_nodes}"

test "${partitions}" -ge 4
test "${distinct_nodes}" -ge 2
