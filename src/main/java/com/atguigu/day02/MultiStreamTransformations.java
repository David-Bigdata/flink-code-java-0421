package com.atguigu.day02;

import com.atguigu.day02.util.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Author：xiaoxin
 * Desc：
 */
public class MultiStreamTransformations {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> tempReadings = env.addSource(new SensorSource());

        // 并行度 设置为 1
        DataStreamSource<SmokleLevel> smokeReadings = env.addSource(new SmokeLevelSource()).setParallelism(1);

        tempReadings.keyBy(r -> r.id)
                .connect(smokeReadings.broadcast())
                .flatMap(new RaiseAlertMap())
                .print();

        env.execute();
    }

    private static class RaiseAlertMap implements CoFlatMapFunction<SensorReading,SmokleLevel, Alert> {
        private SmokleLevel smokleLevel = SmokleLevel.LOW;

        @Override
        public void flatMap1(SensorReading sensorReading, Collector<Alert> collector) throws Exception {
            if(this.smokleLevel == SmokleLevel.HIGH && sensorReading.temperature>0.0){
                collector.collect(new Alert("报警！" + sensorReading,sensorReading.timestamp) );
            }
        }

        @Override
        public void flatMap2(SmokleLevel smokleLevel, Collector<Alert> collector) throws Exception {
            this.smokleLevel = smokleLevel;
        }
    }
}
