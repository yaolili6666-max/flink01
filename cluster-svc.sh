#!/bin/bash
# 管理 Hadoop/Spark/Flink/Kafka/Zookeeper 等集群服务
# 用法: bash cluster-svc.sh <action> [service]
#
# 支持的操作: start | stop | status | restart
# 支持的服务: hadoop | yarn | spark | flink | kafka | zk | maxwell | all

NODES=(hadoop102 hadoop103 hadoop104)
ACTION="$1"
SERVICE="$2"

if [ $# -lt 2 ]; then
    echo "用法: bash cluster-svc.sh <action> <service>"
    echo ""
    echo "操作: start | stop | status | restart"
    echo "服务: hadoop | yarn | spark | flink | kafka | zk | maxwell | all"
    echo ""
    echo "示例:"
    echo "  bash cluster-svc.sh status hadoop"
    echo "  bash cluster-svc.sh start kafka"
    exit 1
fi

# Hadoop HDFS (仅在 hadoop102 操作)
manage_hadoop() {
    case $ACTION in
        start)
            ssh hadoop102 "start-dfs.sh"
            ;;
        stop)
            ssh hadoop102 "stop-dfs.sh"
            ;;
        status)
            for host in "${NODES[@]}"; do
                echo "=== $host JPS ==="
                ssh $host "jps | grep -E 'NameNode|DataNode|SecondaryNameNode'"
            done
            ;;
        restart)
            ssh hadoop102 "stop-dfs.sh && start-dfs.sh"
            ;;
    esac
}

# YARN (仅在 hadoop103 操作 ResourceManager)
manage_yarn() {
    case $ACTION in
        start)
            ssh hadoop103 "start-yarn.sh"
            ;;
        stop)
            ssh hadoop103 "stop-yarn.sh"
            ;;
        status)
            for host in "${NODES[@]}"; do
                echo "=== $host JPS ==="
                ssh $host "jps | grep -E 'ResourceManager|NodeManager'"
            done
            ;;
        restart)
            ssh hadoop103 "stop-yarn.sh && start-yarn.sh"
            ;;
    esac
}

# Kafka (所有节点)
manage_kafka() {
    for host in "${NODES[@]}"; do
        echo "=== Kafka on $host ==="
        case $ACTION in
            start)
                ssh $host "kafka-server-start.sh -daemon \$KAFKA_HOME/config/server.properties"
                ;;
            stop)
                ssh $host "kafka-server-stop.sh"
                ;;
            status)
                ssh $host "jps | grep Kafka"
                ;;
        esac
    done
}

# Zookeeper (所有节点)
manage_zk() {
    for host in "${NODES[@]}"; do
        echo "=== ZK on $host ==="
        case $ACTION in
            start)
                ssh $host "zkServer.sh start"
                ;;
            stop)
                ssh $host "zkServer.sh stop"
                ;;
            status)
                ssh $host "zkServer.sh status"
                ;;
        esac
    done
}

# 所有服务概览
manage_all() {
    echo "==================== 集群服务概览 ===================="
    echo ""
    for host in "${NODES[@]}"; do
        echo "--- $host ---"
        ssh $host "jps 2>/dev/null" 2>/dev/null
        echo ""
    done
}

# 路由到对应服务函数
case $SERVICE in
    hadoop)  manage_hadoop ;;
    yarn)    manage_yarn ;;
    kafka)   manage_kafka ;;
    zk)      manage_zk ;;
    all)     manage_all ;;
    *)
        echo "未知服务: $SERVICE"
        echo "可用服务: hadoop | yarn | kafka | zk | all"
        exit 1
        ;;
esac
