package com.atguigu.day03;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Author：xiaoxin
 * Desc：
 */
public class KeyedProcessFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);

        stream.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, String>> collector) throws Exception {
                String[] arr = s.split(" ");
                collector.collect(Tuple2.of(arr[0],arr[1]));
            }
        })
                .keyBy(r -> r.f0)
                .process(new Mykeyed())
                .print();

        env.execute();
    }


    private static class Mykeyed extends KeyedProcessFunction<String, Tuple2<String, String>, String> {
        //每一条数据过来都会执行一次
        @Override
        public void processElement(Tuple2<String, String> stringStringTuple2, Context context, Collector<String> collector) throws Exception {
            long tenSencondLater = context.timerService().currentProcessingTime() + 5*1000L;
            context.timerService().registerProcessingTimeTimer(tenSencondLater);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("定时器触发了！");
        }
    }
}
