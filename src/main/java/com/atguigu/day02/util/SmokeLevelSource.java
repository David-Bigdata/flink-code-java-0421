package com.atguigu.day02.util;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/**
 * Author：xiaoxin
 * Desc：
 */
public class SmokeLevelSource extends RichParallelSourceFunction<SmokleLevel> {
    private boolean running = true;


    @Override
    public void run(SourceContext<SmokleLevel> sourceContext) throws Exception {
        Random rand = new Random();
        while (running){
            if(rand.nextGaussian()>0.8){
                sourceContext.collect(SmokleLevel.HIGH);
            }else {
                sourceContext.collect(SmokleLevel.LOW);
            }
            Thread.sleep(1000);
        }

    }

    @Override
    public void cancel() {
        this.running = false;

    }
}
