package test.wc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 使用 Flink SQL 实现 WordCount
 * 与 WorldCountTable（Table API）的区别：这里全部用 SQL 字符串完成建表和查询
 */
public class WorldCountSql {
    public static void main(String[] args) throws Exception {
        // 1. 构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 2. 创建数据输入表（datagen 连接器，word 取值 1~5，方便观察累计计数）
        tEnv.executeSql(
                "CREATE TABLE source (\n" +
                "    word INT\n" +
                ") WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'rows-per-second' = '1',\n" +
                "    'fields.word.kind' = 'random',\n" +
                "    'fields.word.min' = '1',\n" +
                "    'fields.word.max' = '5'\n" +
                ")"
        );

        // 3. 创建数据输出表（print 连接器，直接打印到控制台）
        tEnv.executeSql(
                "CREATE TABLE sink (\n" +
                "    word INT,\n" +
                "    cnt BIGINT\n" +
                ") WITH (\n" +
                "    'connector' = 'print'\n" +
                ")"
        );

        // 4. 使用 SQL 进行单词统计，并将结果插入输出表（executeSql 已触发作业执行）
        tEnv.executeSql(
                "INSERT INTO sink " +
                "SELECT word, COUNT(*) AS cnt " +
                "FROM source " +
                "GROUP BY word"
        );
    }
}
