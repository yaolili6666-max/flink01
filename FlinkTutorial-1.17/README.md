# FlinkTutorial-1.17

基于 **Apache Flink 1.17.0** 的完整学习教程，覆盖 DataStream API、状态管理、Checkpoint/Savepoint、Table/SQL API 四大核心模块，共 **73 个示例程序**，按由浅入深的学习路径编排。

> 教程配套尚硅谷 Flink 1.17 视频课程，所有代码均经过真机验证。

---

## 目录

- [环境要求](#环境要求)
- [快速开始](#快速开始)
- [项目结构](#项目结构)
- [学习路径](#学习路径)
  - [1. POJO 模型](#1-pojo-模型)
  - [2. WordCount 入门 & 运行链](#2-wordcount-入门--运行链)
  - [3. 执行环境](#3-执行环境)
  - [4. Source 数据源](#4-source-数据源)
  - [5. 可复用 UDF](#5-可复用-udf)
  - [6. 基本转换算子](#6-基本转换算子)
  - [7. KeyBy 与聚合](#7-keyby-与聚合)
  - [8. 物理分区](#8-物理分区)
  - [9. 分流](#9-分流)
  - [10. 合流](#10-合流)
  - [11. Sink 输出](#11-sink-输出)
  - [12. 窗口](#12-窗口)
  - [13. Watermark & 事件时间](#13-watermark--事件时间)
  - [14. ProcessFunction](#14-processfunction)
  - [15. 状态管理](#15-状态管理)
  - [16. Checkpoint & Savepoint](#16-checkpoint--savepoint)
  - [17. Table API & SQL](#17-table-api--sql)
- [依赖说明](#依赖说明)
- [打包部署](#打包部署)
- [常用命令](#常用命令)

---

## 环境要求

| 组件 | 版本 | 说明 |
|------|------|------|
| JDK | 1.8+ | 推荐 JDK 8 |
| Maven | 3.6+ | 依赖管理与构建 |
| Flink | 1.17.0 | 核心框架 |
| Kafka | 3.x | 部分 Source/Sink 示例需要 |
| MySQL | 8.x | JDBC Sink 示例需要 |
| Hadoop | 3.3.x | Checkpoint 持久化到 HDFS 时需要 |

---

## 快速开始

```bash
# 1. 克隆项目
git clone <repo-url>
cd FlinkTutorial-1.17

# 2. 编译打包（跳过测试）
mvn clean package -DskipTests

# 3. 在 IDE 中直接运行任意 main() 方法
#    推荐先运行 com.atguigu.wc.WordCountStreamDemo
```

> **⚠️ 注意**：在 IDE 运行时，`pom.xml` 中 `flink-clients` 的 scope 已设为 `compile`，确保本地执行。打包上集群时改回 `provided`（集群环境自带此 jar）。

---

## 项目结构

```
FlinkTutorial-1.17/
├── pom.xml
├── README.md
└── src/main/java/com/atguigu/
    ├── bean/                    # POJO 模型
    ├── wc/                      # WordCount 入门 & 运行机制
    ├── env/                     # 执行环境
    ├── source/                  # 数据源 (4 种)
    ├── functions/               # 可复用 UDF (3 个)
    ├── transfrom/               # 基本转换 (map/flatMap/filter/rich)
    ├── aggreagte/               # KeyBy 与聚合
    ├── partition/               # 物理分区 & 自定义分区器
    ├── split/                   # 分流（filter 方式 vs Side Output）
    ├── combine/                 # 合流（union/connect/手工 join）
    ├── sink/                    # 输出（文件/Kafka/MySQL/自定义）
    ├── window/                  # 时间窗口 & 计数窗口
    ├── watermark/               # 水位线 & 事件时间 & Join
    ├── process/                 # ProcessFunction & TopN
    ├── state/                   # 状态管理 & 状态后端
    ├── checkpoint/              # Checkpoint / Savepoint / Kafka EOS
    └── sql/                     # Table API & SQL & 自定义函数
```

---

## 学习路径

### 1. POJO 模型

| 文件 | 说明 |
|------|------|
| `bean/WaterSensor.java` | 核心数据模型：`id`(传感器ID), `ts`(时间戳), `vc`(水位值)。几乎所有 Demo 共用 |

---

### 2. WordCount 入门 & 运行链

| 文件 | 类型 | 知识点 |
|------|------|--------|
| `wc/WordCountBatchDemo.java` | Batch (DataSet) | 旧版批处理 API，已不推荐使用 |
| `wc/WordCountStreamDemo.java` | Streaming（有界流） | DataStream WordCount，文件源可离线运行 |
| `wc/WordCountStreamUnboundedDemo.java` | Streaming（无界流） | Socket 源，演示并行度设置规则 |
| `wc/OperatorChainDemo.java` | 运行机制 | 算子链概念与控制：`disableChaining()` / `startNewChain()` |
| `wc/SlotSharingGroupDemo.java` | 资源调度 | Slot 共享组机制，并行度与 Slot 数关系 |

---

### 3. 执行环境

| 文件 | 知识点 |
|------|--------|
| `env/EnvDemo.java` | 四种获取执行环境方式、运行时模式切换（STREAMING/BATCH）、`execute()` vs `executeAsync()` |

---

### 4. Source 数据源

| 文件 | 知识点 |
|------|--------|
| `source/CollectionDemo.java` | 集合/元素直接构造流：`fromElements()` / `fromCollection()` |
| `source/FileSourceDemo.java` | **新版**文件源：`FileSource.forRecordStreamFormat()` + `env.fromSource()` |
| `source/DataGeneratorDemo.java` | **新版**数据生成源：`DataGeneratorSource` 配置、并行度分片机制 |
| `source/KafkaSourceDemo.java` | **新版**Kafka 源：`KafkaSource` + Watermark 策略 + offset 控制 |

---

### 5. 可复用 UDF

| 文件 | 类型 | 说明 |
|------|------|------|
| `functions/WaterSensorMapFunction.java` | `MapFunction<String, WaterSensor>` | 解析逗号分隔字符串 → WaterSensor |
| `functions/MapFunctionImpl.java` | `MapFunction<WaterSensor, String>` | 提取传感器 ID |
| `functions/FilterFunctionImpl.java` | `FilterFunction<WaterSensor>` | 按 ID 过滤（构造器参数） |

---

### 6. 基本转换算子

| 文件 | 知识点 |
|------|--------|
| `transfrom/MapDemo.java` | Map：匿名类 / Lambda / 独立类 三种写法 |
| `transfrom/FlatmapDemo.java` | FlatMap：一对多/零输出/过滤效果，`Collector` vs `return` 区别 |
| `transfrom/FilterDemo.java` | Filter：使用参数化 UDF 过滤 |
| `transfrom/RichFunctionDemo.java` | RichFunction：`open()`/`close()` 生命周期、`RuntimeContext` 获取子任务信息 |

---

### 7. KeyBy 与聚合

| 文件 | 知识点 |
|------|--------|
| `aggreagte/KeybyDemo.java` | KeyBy 不是转换而是重分区；同 key 同分区；一个分区可包含多个 key |
| `aggreagte/SimpleAggregateDemo.java` | sum/max/min/maxBy/minBy；`max` 与 `maxBy` 的区别 |
| `aggreagte/ReduceDemo.java` | Reduce 两两归并逻辑；按 key 独立维护中间结果 |

---

### 8. 物理分区

| 文件 | 知识点 |
|------|--------|
| `partition/PartitionDemo.java` | 7 种内置分区器：shuffle / rebalance / rescale / broadcast / global / forward / keyBy |
| `partition/MyPartitioner.java` | 自定义分区器：按整数取模分配 |
| `partition/PartitionCustomDemo.java` | `partitionCustom()` 应用自定义分区器 |

---

### 9. 分流

| 文件 | 知识点 |
|------|--------|
| `split/SplitByFilterDemo.java` | 用多次 filter 实现分流（缺点：数据被处理多次） |
| `split/SideOutputDemo.java` | **推荐方式**：ProcessFunction + OutputTag 侧输出流，一次处理完成分流 |

---

### 10. 合流

| 文件 | 知识点 |
|------|--------|
| `combine/UnionDemo.java` | Union：合并**同类型**流 |
| `combine/ConnectDemo.java` | Connect：连接**不同类型**流 + CoMapFunction |
| `combine/ConnectKeybyDemo.java` | Connect + KeyBy + CoProcessFunction 实现**手工双流 Join**（Inner Join 效果） |

---

### 11. Sink 输出

| 文件 | 知识点 |
|------|--------|
| `sink/SinkFile.java` | 新版 FileSink：分桶策略 / 文件滚动策略 / 需要 Checkpoint |
| `sink/SinkKafka.java` | Kafka Sink：**精确一次**语义、事务配置 |
| `sink/SinkKafkaWithKey.java` | Kafka Sink + 自定义 Key |
| `sink/SinkMySQL.java` | JDBC Sink：batch 提交 / 重试机制 |
| `sink/SinkCustom.java` | 自定义 Sink：RichSinkFunction 模板 |

---

### 12. 窗口

| 文件 | 知识点 |
|------|--------|
| `window/WindowApiDemo.java` | 窗口 API 全景：WindowAssigner / WindowFunction 分类总览 |
| `window/TimeWindowDemo.java` | 三大时间窗口：滚动（10s）/ 滑动（10s+5s）/ 会话（固定间隙 & 动态间隙） |
| `window/CountWindowDemo.java` | 计数窗口：滚动 `countWindow(5)` / 滑动 `countWindow(5,2)` |
| `window/WindowReduceDemo.java` | 增量聚合：ReduceFunction 在窗口中的应用 |
| `window/WindowAggregateDemo.java` | 增量聚合：AggregateFunction 灵活类型转换 |
| `window/WindowProcessDemo.java` | 全量聚合：ProcessWindowFunction 获取窗口元数据 |
| `window/WindowAggregateAndProcessDemo.java` | **增量+全量结合**：aggregate 省内存 + process 查元数据的组合模式 |

---

### 13. Watermark & 事件时间

| 文件 | 知识点 |
|------|--------|
| `watermark/WatermarkMonoDemo.java` | 有序流：`forMonotonousTimestamps()` + 事件时间滚动窗口 |
| `watermark/WatermarkOutOfOrdernessDemo.java` | 乱序流：`forBoundedOutOfOrderness(3s)`；多并行度下 Watermark 传播（取最小值） |
| `watermark/MyPeriodWatermarkGenerator.java` | 自定义周期性 Watermark 生成器 |
| `watermark/MyPuntuatedWatermarkGenerator.java` | 自定义断点式 Watermark 生成器 |
| `watermark/WatermarkCustomDemo.java` | 使用自定义 Watermark 生成器 |
| `watermark/WatermarkIdlenessDemo.java` | 空闲源处理：`withIdleness(5s)` 防止 Watermark 停滞 |
| `watermark/WatermarkLateDemo.java` | 迟到数据处理三重奏：Watermark 延迟等待 → allowedLateness 重新触发 → SideOutput 兜底 |
| `watermark/WindowJoinDemo.java` | 基于事件时间的窗口 Join |
| `watermark/IntervalJoinDemo.java` | 基于事件时间间隔的 Interval Join（不受窗口边界约束） |
| `watermark/IntervalJoinWithLateDemo.java` | Interval Join + 迟到数据侧输出处理 |

---

### 14. ProcessFunction

| 文件 | 知识点 |
|------|--------|
| `process/KeyedProcessTimerDemo.java` | 定时器注册与触发机制、Watermark 驱动关系 |
| `process/ProcessAllWindowTopNDemo.java` | TopN（windowAll 方式）：单并行度全局统计 + 排序 |
| `process/KeyedProcessFunctionTopNDemo.java` | TopN（**生产级做法**）：两阶段——窗口聚合 + 按窗口标记分组 + 定时器触发排序输出 |
| `process/SideOutputDemo.java` | ProcessFunction 中侧输出用于告警/监控 |

---

### 15. 状态管理

| 文件 | 知识点 |
|------|--------|
| `state/KeyedValueStateDemo.java` | ValueState：连续值差异告警 |
| `state/KeyedListStateDemo.java` | ListState：每传感器维护 Top 3 水位峰值 |
| `state/KeyedMapStateDemo.java` | MapState：每传感器统计各水位值出现次数 |
| `state/KeyedReducingStateDemo.java` | ReducingState：每传感器水位值累加 |
| `state/KeyedAggregatingStateDemo.java` | AggregatingState：每传感器水位均值计算 |
| `state/OperatorListStateDemo.java` | Operator State：CheckpointedFunction 接口、rescale 时 `ListState` vs `UnionListState` 的区别 |
| `state/OperatorBroadcastStateDemo.java` | Broadcast State：动态下发阈值配置，主流只读 / 广播流读写 |
| `state/StateTTLDemo.java` | State TTL：5 秒过期配置，`OnReadAndWrite` 更新 / `NeverReturnExpired` 可见性 |
| `state/StateBackendDemo.java` | HashMapStateBackend（堆内存） vs EmbeddedRocksDBStateBackend（磁盘） |

---

### 16. Checkpoint & Savepoint

| 文件 | 知识点 |
|------|--------|
| `checkpoint/CheckpointConfigDemo.java` | 完整 Checkpoint 配置：间隔/超时/并发/暂停/外部化/容错数 + 非对齐检查点 + Changelog 后端 |
| `checkpoint/SavepointDemo.java` | Savepoint：每个算子设置 `uid()` + `name()`，保证跨版本兼容 |
| `checkpoint/KafkaEOSDemo.java` | **端到端精确一次**：Kafka Source + Kafka Sink 2PC 事务 |
| `checkpoint/KafkaEOSDemo2.java` | 消费 2PC 事务数据：设置 `read_committed` 隔离级别 |

---

### 17. Table API & SQL

| 文件 | 知识点 |
|------|--------|
| `sql/SqlDemo.java` | Table/SQL 入门：建源表（datagen）/ 建结果表（print）/ SQL 查询 / Table API 查询 |
| `sql/TableStreamDemo.java` | 流表互转：`fromDataStream` → Table → SQL → `toDataStream` / `toChangelogStream` |
| `sql/MyScalarFunctionDemo.java` | **标量函数 UDF**：自定义 `ScalarFunction`，输入一行输出一行 |
| `sql/MyTableFunctionDemo.java` | **表函数 UDTF**：自定义 `TableFunction<Row>`，`LATERAL TABLE` 侧连接 |
| `sql/MyAggregateFunctionDemo.java` | **聚合函数 UDAF**：自定义 `AggregateFunction`，多行输入一行输出 |
| `sql/MyTableAggregateFunctionDemo.java` | **表聚合函数 UDTAGG**：自定义 `TableAggregateFunction`，多行输入多行输出 + TopN 实战 |

---

## 依赖说明

| 依赖 | Scope | 说明 |
|------|-------|------|
| `flink-streaming-java` | provided | 核心流处理 API（集群已提供） |
| `flink-clients` | compile | 本地执行器（IDE 运行必需） |
| `flink-runtime-web` | provided | Web UI 调试工具 |
| `flink-connector-files` | provided | 文件连接器 |
| `flink-connector-kafka` | compile | Kafka 连接器 |
| `flink-connector-datagen` | compile | 数据生成连接器 |
| `flink-connector-jdbc` | compile | JDBC 连接器（快照版） |
| `mysql-connector-java` | compile | MySQL 驱动 |
| `flink-statebackend-rocksdb` | provided | RocksDB 状态后端 |
| `hadoop-client` | provided | HDFS 文件系统客户端 |
| `flink-table-api-java-bridge` | compile | Table API 桥接 |
| `flink-table-planner-loader` | compile | Table 规划器 |
| `flink-table-runtime` | compile | Table 运行时 |

---

## 打包部署

```bash
# 使用 maven-shade-plugin 打胖包
mvn clean package -DskipTests

# 生成的 jar 位于 target/ 目录
# FlinkTutorial-1.17-1.0-SNAPSHOT.jar

# 提交到 Flink 集群（示例）
flink run -c com.atguigu.wc.WordCountStreamDemo target/FlinkTutorial-1.17-1.0-SNAPSHOT.jar
```

> ⚠️ 打包上集群前，建议将 `flink-clients` 的 scope 改回 `provided`，避免 jar 冲突。

---

## 常用命令

```bash
# Flink 集群操作（在集群节点上）
start-cluster.sh                 # 启动 Flink 集群
stop-cluster.sh                  # 停止 Flink 集群
flink list                       # 查看运行中的作业
flink cancel <job-id>            # 取消作业
flink cancel -s <job-id>         # 取消作业并触发 Savepoint
flink run -s <savepoint-path> <jar>  # 从 Savepoint 恢复

# Kafka 操作
kafka-console-consumer.sh --bootstrap-server hadoop102:9092 --topic ws
kafka-console-producer.sh --bootstrap-server hadoop102:9092 --topic topic_1

# netcat（用于 Socket Source 示例）
nc -lk 7777
```

---

**API 覆盖统计**：62 个 DataStream 示例 + 1 个 DataSet 示例 + 6 个 Table/SQL 示例 + 4 个 UDF/工具类 = **73 个文件**，横跨 17 个知识模块。
