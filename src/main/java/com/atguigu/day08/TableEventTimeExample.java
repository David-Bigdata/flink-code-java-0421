package com.atguigu.day08;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * Author：xiaoxin
 * Desc：
 */
public class TableEventTimeExample {
    public static void main(String[] args) throws Exception {
        // 创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //使用流模式
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        //创建表环境
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);
        //数据源
        SingleOutputStreamOperator<SensorReading> stream = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                                    @Override
                                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        Table table = tenv.fromDataStream(
                stream,
                $("id"),
                $("timestamp").rowtime().as("ts"),
                $("temperature"));

        Table result = table.window(Tumble.over(lit(10).seconds()).on($("ts")).as("win"))
                .groupBy($("id"), $("win"))
                .select($("id"), $("id").count());

        tenv.createTemporaryView(
                "sensor",
                stream,
                $("id"),
                $("timestamp").rowtime().as("ts"),
                $("temperature"));

        Table sqlresult = tenv.sqlQuery("SELECT id, COUNT(id) FROM sensor WHERE id = 'sensor_1' GROUP BY id, TUMBLE(ts, INTERVAL '10' SECOND)");
        tenv.toRetractStream(sqlresult, Row.class).print();

        env.execute();
    }
}
