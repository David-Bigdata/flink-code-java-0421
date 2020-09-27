package com.atguigu.day02.util;

/**
 * Author：xiaoxin
 * Date：2020/9/27
 * Desc：
 */
public class SensorReading {
    public String id ;
    public Long timestamp;
    public Double temperature;

    public SensorReading() {
    }

    public SensorReading(String id, Long timestamp, Double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }
}
