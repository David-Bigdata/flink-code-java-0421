package com.atguigu.day08;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
public class FlinkTableExample {
    public static void main(String[] args) throws Exception {
        // 创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        Table table = tenv.fromDataStream(
                stream,
                $("id"),
                $("timestamp").as("ts"),
                $("temperature"),
                $("pt").proctime());
        Table result = table.window(Tumble.over(lit(10).seconds()).on($("pt")).as("win"))
                .groupBy($("id"), $("win"))
                .select($("id"), $("id").count());

        tenv.createTemporaryView(
                "sensor",
                stream,
                $("id"),
                $("timestamp").as("ts"),
                $("temperature"),
                $("pt").proctime());

        Table sqlresult = tenv.sqlQuery("SELECT id, COUNT(id) FROM sensor GROUP BY id, TUMBLE(pt, INTERVAL '10' SECOND)");
        tenv.toRetractStream(sqlresult, Row.class).print();

        env.execute();
    }
}
