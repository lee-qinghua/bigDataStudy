package com.otis.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import scala.tools.nsc.transform.patmat.ScalaLogic;

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

        //添加source
        DataStreamSource<String> dataStream = env.readTextFile("/");

        //创建table
        tableEnv.createTemporaryView("inputtable", dataStream, "id,name");

        Table table = tableEnv.from("inputtable");
        Table resultTable1 = table
                .filter("country==='China'")
                .groupBy("cid,cname")
                .select("cid,cname,age.sum as agesum");

        //定义schema
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.INT());
        //注册视图
        tableEnv.connect(new FileSystem().path("/"))
                .withFormat(new Csv())
                .withSchema(schema)
                .createTemporaryTable("output_table");

        //插入视图就是sink到了外部系统
        table.insertInto("output_table");



    }
}
