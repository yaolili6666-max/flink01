package test.flinksql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 从datagen造数据，模拟数据sink到Kafka分区（累加模式）
 * source表：商品id、销售价格
 * sink表：商品id、销售件数、销售金额、最高销售金额、最低销售金额（持续累加）
 *
 * @author 黑大帅
 * @create 2026/06/21
 */
public class Sink2Kafka {
    public static void main(String[] args) throws Exception {
        // 1. 构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 2. 创建 source 表（datagen 连接器，模拟商品销售流水数据）
        //    goods_id: 商品id（1~5随机），price: 销售价格（1~100随机）
        tEnv.executeSql(
                "CREATE TABLE source (\n" +
                "    goods_id INT,\n" +
                "    price    DOUBLE\n" +
                ") WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'rows-per-second' = '5',\n" +
                "    'fields.goods_id.kind' = 'random',\n" +
                "    'fields.goods_id.min' = '1',\n" +
                "    'fields.goods_id.max' = '5',\n" +
                "    'fields.price.kind' = 'random',\n" +
                "    'fields.price.min' = '1',\n" +
                "    'fields.price.max' = '100'\n" +
                ")"
        );

        // 3. 创建 sink 表（upsert-kafka 连接器，按 goods_id 持续累加更新）
        //    upsert-kafka 要求：PRIMARY KEY、key.format、value.format
        tEnv.executeSql(
                "CREATE TABLE sink (\n" +
                "    goods_id   INT,\n" +
                "    cnt        BIGINT,\n" +
                "    amount     DOUBLE,\n" +
                "    max_amount DOUBLE,\n" +
                "    min_amount DOUBLE,\n" +
                "    PRIMARY KEY (goods_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = 'goods_sales',\n" +
                "    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n" +
                "    'key.format' = 'json',\n" +
                "    'value.format' = 'json'\n" +
                ")"
        );

        // 4. 累加模式：无窗口 GROUP BY，每来一条数据就更新结果（cnt 只增不减）
        tEnv.executeSql(
                "INSERT INTO sink " +
                "SELECT goods_id, COUNT(*) AS cnt, SUM(price) AS amount, " +
                "       MAX(price) AS max_amount, MIN(price) AS min_amount " +
                "FROM source " +
                "GROUP BY goods_id"
        );
    }
}
