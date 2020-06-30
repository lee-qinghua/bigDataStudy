package com.qinghua.java.quickStart;

import com.otis.flinksql.quickStart.SensorReading;
import com.otis.flinksql.quickStart.SensorSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class Demo1 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());
        //创建表环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

//        tableEnvironment.sqlQuery()

        //把流转换成表
        Table table = tableEnvironment.fromDataStream(stream);

        Table resultTable = table.select("id,temperature")
                .filter("id == 'sensor_1'");

    }
}
