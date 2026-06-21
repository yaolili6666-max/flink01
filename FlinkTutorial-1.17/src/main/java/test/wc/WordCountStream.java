package test.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * DataStream API 实现 WordCount：Socket 无界流
 * <p>
 * 先在 Xshell 中启动 netcat 发送数据：
 * nc -lk hadoop102 7777
 * 然后提交 Flink 作业，在 netcat 端输入单词即可看到实时统计结果
 *
 * @author 黑大帅
 * @create 2026/06/21
 */
public class WordCountStream {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取 Socket 文本流（无界流）
        //    参数1：主机名，参数2：端口号
        //    在 Xshell 中执行 nc -lk hadoop102 7777 即可输入数据
        DataStreamSource<String> lineDS = env.socketTextStream("hadoop102", 7777);

        // 3. 处理：切分 → 转换 → 分组 → 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = lineDS
                // 3.1 切分 + 转换：(word, 1)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
                        for (String word : line.split(" ")) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                // 3.2 按单词分组
                .keyBy(0)
                // 3.3 聚合求和
                .sum(1);

        // 4. 输出
        sumDS.print();

        // 5. 执行
        env.execute("DataStream WordCount");
    }
}
