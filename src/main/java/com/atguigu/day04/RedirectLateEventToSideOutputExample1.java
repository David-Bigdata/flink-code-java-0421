package com.atguigu.day04;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author：xiaoxin
 * Desc：
 */
public class RedirectLateEventToSideOutputExample1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        env.socketTextStream("localhost",9999);

        env.execute();
    }
}
