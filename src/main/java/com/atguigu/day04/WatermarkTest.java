package com.atguigu.day04;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Author：xiaoxin
 * Desc：
 */
public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);

        stream
                //Tuple2<key, timestamp>  毫秒时间设置
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] arr = s.split(" ");
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]) * 1000L);
                    }
                })
                //默认200ms 机器时间 插入一次水位线
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                        .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> r, long l) {
                                return r.f1;
                            }
                        })
                )
                .keyBy(r ->r.f0)
                .process(new Keyed())
                .print();


        env.execute();
    }

    private static class Keyed extends KeyedProcessFunction<String, Tuple2<String, Long>, String> {
        @Override
        public void processElement(Tuple2<String, Long> stringLongTuple2, Context context, Collector<String> collector) throws Exception {
            collector.collect("当前水位线"+context.timerService().currentWatermark());
        }
    }
}
