package test.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * DataSet API 实现 WordCount：批处理方式读取文件
 * 注意：DataSet API 在 Flink 1.17 中已不推荐使用，建议使用 DataStream API 的 BATCH 模式
 *
 * @author test
 * @version 1.0
 */
public class WordCountBatch {
    public static void main(String[] args) throws Exception {
        // 1. 创建批处理执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取文件
        DataSource<String> lineDS = env.readTextFile("input/word.txt");

        // 3. 切分 + 转换：(word, 1)
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = lineDS.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
                        for (String word : line.split(" ")) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                }
        );

        // 4. 按单词分组（按 Tuple2.f0 下标分组）
        UnsortedGrouping<Tuple2<String, Integer>> grouped = wordAndOne.groupBy(0);

        // 5. 聚合求和（下标 1 表示 Tuple2.f1）
        AggregateOperator<Tuple2<String, Integer>> sum = grouped.sum(1);

        // 6. 输出（DataSet.print() 内部会触发 execute）
        sum.print();
    }
}
