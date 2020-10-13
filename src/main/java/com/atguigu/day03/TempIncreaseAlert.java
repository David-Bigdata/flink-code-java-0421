package com.atguigu.day03;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

;


/**
 * Author：xiaoxin
 * Desc：
 */
public class TempIncreaseAlert {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        stream
                .keyBy(r -> r.id)
                .process(new TempIncrease())
                .print();

        env.execute();
    }

    private static class TempIncrease extends KeyedProcessFunction<String, SensorReading, String> {
        private ValueState<Double> lastTemp;
        private ValueState<Long> currentTimer;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            lastTemp = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>("last-temp", Types.DOUBLE)
            );
            currentTimer = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("timer", Types.LONG)
            );
        }

        @Override
        public void processElement(SensorReading r, Context ctx, Collector<String> collector) throws Exception {

            Double prevTemp = 0.0;

            if(lastTemp.value() != null){
                prevTemp = lastTemp.value();
            }

            lastTemp.update(r.temperature);

            Long curTimerTimestamp = 0L;

            if(currentTimer.value() != null){
                curTimerTimestamp = currentTimer.value();
            }

            if (prevTemp == 0.0 || r.temperature < prevTemp){
                ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp);
                currentTimer.clear();
            }else if(r.temperature>prevTemp && curTimerTimestamp ==0L){
                long oneSecondLater = ctx.timerService().currentProcessingTime() + 1000L;

                ctx.timerService().registerProcessingTimeTimer(oneSecondLater);

                currentTimer.update(oneSecondLater);

            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);

            out.collect("传感器id为：" + ctx.getCurrentKey() + "的传感器连续1s温度上升！");
            currentTimer.clear();
        }
    }
}
