package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * Author：xiaoxin
 * Date：2020/9/26
 * Desc：
 */
public class WorderCountFromSocket {
    public static void main(String[] args) throws Exception {
        //建立连接
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行任务为1
        env.setParallelism(1);

        DataStream<String> stream = env.socketTextStream("localhost",9999);


        SingleOutputStreamOperator<Tuple2<String, Integer>> result = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] arr = s.split(" ");
                for (String e : arr) {
                    collector.collect(Tuple2.of(e, 1));
                }
            }
            })
            .keyBy(r -> r.f0)
            .sum(1);
        result.print();


        //执行任务  语句
        env.execute();


    }
}
