package com.atguigu.partition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * TODO
 *
 * @author 黑大帅
 * @create 2026/06/21
 */
public class MyPartitioner implements Partitioner<String> {
    @Override
    public int partition(String key, int numPartitions) {
        return Integer.parseInt(key) % numPartitions;
    }
}
