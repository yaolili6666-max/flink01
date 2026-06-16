#!/bin/bash
# 将文件分发到所有集群节点
# 用法: bash cluster-scp.sh <local-file> <remote-path>

NODES="hadoop102 hadoop103 hadoop104"

if [ $# -lt 2 ]; then
    echo "用法: bash cluster-scp.sh <本地文件> <远程路径>"
    echo "示例: bash cluster-scp.sh ./test.jar /home/hds/"
    exit 1
fi

for host in $NODES; do
    echo "=== 分发到 $host ==="
    scp "$1" $host:"$2"
    if [ $? -eq 0 ]; then
        echo "  ✓ $host 完成"
    else
        echo "  ✗ $host 失败"
    fi
done
