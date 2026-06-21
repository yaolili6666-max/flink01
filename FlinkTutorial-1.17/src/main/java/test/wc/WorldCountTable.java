package test.wc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 使用 Table API 实现 WordCount
 * 思路：DataStream 读取+切分 → 转 Table → Table API 分组聚合 → 转流输出
 *
 * @author 黑大帅
 * @create 2026/06/21
 */
public class WorldCountTable {
    public static void main(String[] args) throws Exception {
        // 1. 构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //2.数据输入(数据输入表)
        tEnv.createTemporaryTable("source", TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder()
                        .column("word", DataTypes.INT())
                        .build())
                .option("rows-per-second", "1")
                .option("fields.word.kind", "random")
                .option("fields.word.min", "1")
                .option("fields.word.max", "5")
                .build());
        //3.数据输出(数据输出表)
        tEnv.createTemporaryTable("sink", TableDescriptor.forConnector("print")
                .schema(Schema.newBuilder()
                        .column("word", DataTypes.INT())
                        .column("cnt", DataTypes.BIGINT())
                        .build())
                .build());

        //4.数据处理(基于数据输入表、数据输出表进行业务处理(单词统计)
        Table table = tEnv.from("source")
                .groupBy($("word"))
                .select($("word"), $("word").count().as("cnt"));

        // 将 Table 结果写入 sink 表（executeInsert 已触发作业执行，无需再调用 env.execute()）
        table.executeInsert("sink");


    }
}

