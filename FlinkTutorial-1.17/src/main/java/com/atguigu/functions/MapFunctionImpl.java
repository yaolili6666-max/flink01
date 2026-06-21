package com.atguigu.functions;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * TODO
 *
 * @author 黑大帅
 * @create 2026/06/21
 */
public class MapFunctionImpl implements MapFunction<WaterSensor,String> {
    @Override
    public String map(WaterSensor value) throws Exception {
        return value.getId();
    }
}
