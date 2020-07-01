package com.otis.quickStart;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Demo1 {
    public static void main(String[] args) {
        //创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //创建表环境
        // **********************
        // BLINK STREAMING QUERY
        // **********************
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // **********************
        // BLINK BATCH QUERY
        // **********************
        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment bbTableEnv = TableEnvironment.create(bbSettings);

        //添加source
        DataStreamSource<String> dataStream = env.readTextFile("/");

        //创建table
        tableEnv.createTemporaryView("inputtable", dataStream);

        Table table = tableEnv.from("inputtable");
        Table resultTable1 = table
                .filter("country==='China'")
                .groupBy("cid,cname")
                .select("cid,cname,age.sum as agesum");

        Table resultTable2 = tableEnv.sqlQuery(
                "select cid,cname,sum(age) as sumage" +
                        "from inputtable" +
                        "group by cid,cname");

        //插入表
        tableEnv.sqlUpdate(
                "INSERT INTO RevenueFrance " +
                        "SELECT cID, cName, SUM(revenue) AS revSum " +
                        "FROM Orders " +
                        "WHERE cCountry = 'FRANCE' " +
                        "GROUP BY cID, cName"
        );

        //table转成DataStream
        DataStream<Row> stream1 = tableEnv.toAppendStream(table, Row.class);
        DataStream<Tuple2<Boolean, Row>> stream2 = tableEnv.toRetractStream(table, Row.class);


        //stream映射表的字段 比如stream中有两个字段 id name
        Table table1 = tableEnv.fromDataStream(stream1, "id"); //只取一个字段
        Table table2 = tableEnv.fromDataStream(stream1, "name,id"); //字段位置变化
        Table table3 = tableEnv.fromDataStream(stream1, "name as myName,id as myId"); //字段位置变化

    }
}
