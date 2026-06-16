#!/bin/bash
# 在所有集群节点上批量执行命令
# 用法: bash cluster-exec.sh "<command>"

NODES="hadoop102 hadoop103 hadoop104"

if [ $# -eq 0 ]; then
    echo "用法: bash cluster-exec.sh \"<命令>\""
    echo "示例: bash cluster-exec.sh \"jps\""
    exit 1
fi

for host in $NODES; do
    echo "============================================="
    echo "=== $host ==="
    echo "============================================="
    ssh $host "$@"
    echo ""
done
