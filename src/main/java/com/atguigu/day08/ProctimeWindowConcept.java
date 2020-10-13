package com.atguigu.day08;


import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Author：xiaoxin
 * Desc：
 */
public class ProctimeWindowConcept {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        Table table = tenv.fromDataStream(
                stream,
                $("id"),
                $("id").as("ts"),
                $("temperature"),
                $("pt").proctime());

        //        table.window(Tumble.over(lit(10).seconds()).on($("pt")).as("win"))
        //        table.window(Slide.over(lit(10).seconds()).every(lit(5).seconds()).on($("pt")).as("win"))

        tenv.createTemporaryView(
                "sensor",
                stream,
                $("id"),
                $("timestamp").as("ts"),
                $("temperature"),
                $("pt").proctime());

        //滚动窗口
        Table sqlTumbleWindowResult = tenv.sqlQuery("SELECT id, count(id), TUMBLE_START(pt, INTERVAL '10' SECOND), TUMBLE_END(pt, INTERVAL '10' SECOND) FROM sensor WHERE id = 'sensor_1' GROUP BY id, TUMBLE(pt, INTERVAL '10' SECOND)");
        tenv.toRetractStream(sqlTumbleWindowResult,Row.class).print();

        //滑动窗口
        Table sqlSlideWindowResult = tenv.sqlQuery("SELECT id, count(id), HOP_START(pt, INTERVAL '5' SECOND, INTERVAL '10' SECOND), HOP_END(pt, INTERVAL '5' SECOND, INTERVAL '10' SECOND) FROM sensor WHERE id = 'sensor_1' GROUP BY id, HOP(pt, INTERVAL '5' SECOND, INTERVAL '10' SECOND)");
        tenv.toRetractStream(sqlSlideWindowResult,Row.class).print();

        env.execute();
    }
}
