package com.otis.time;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.List;
//https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/streaming/time_attributes.html
public class Demo {
    public static void main(String[] args) {

        //创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //创建表环境

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        //*************
        //todo Defining in create table DDL
        //*************
        String ddl = "create table user_actions(" +
                "user_name string," +
                "data string," +
                "user_action_time as proctime()" +
                ")with(" +
                "'connector.type' = 'filesystem'," +
                " 'connector.path' = 'file:///D:\\\\..\\\\sensor.txt'," +
                "'format.type' = 'csv'" +
                ")";
        //SELECT TUMBLE_START(user_action_time, INTERVAL '10' MINUTE), COUNT(DISTINCT user_name)
        //FROM user_actions
        //GROUP BY TUMBLE(user_action_time, INTERVAL '10' MINUTE);
        tableEnv.sqlUpdate(ddl);


        //*************
        //todo During DataStream-to-Table Conversion
        //*************

        DataStream<Tuple2<String, String>> stream = null;
        // declare an additional logical field as a processing time attribute
        Table table = tableEnv.fromDataStream(stream, "user_name, data, user_action_time.proctime");
        Table minTable = table.window(Tumble.over("10.minutes").on("user_action_time").as("userActionWindow"))
                .groupBy("userActionWindow,user_name")
                .select("user_name");

        //*************
        //todo 自定义带时间属性的source
        //*************
        tableEnv.createTemporaryView("user_actions", (Table) new UserActionSource());
        GroupWindowedTable window = tableEnv
                .from("user_actions")
                .window(Tumble.over("10.minutes").on("user_action_time").as("userActionWindow"));
    }

    //自定义一个带时间属性的tableSource
    static class UserActionSource implements StreamTableSource<Row>, DefinedRowtimeAttributes {

        @Override
        public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
            // Mark the "user_action_time" attribute as event-time attribute.
            // We create one attribute descriptor of "user_action_time".
            RowtimeAttributeDescriptor rowtimeAttrDescr = new RowtimeAttributeDescriptor(
                    "user_action_time",
                    new ExistingField("user_action_time"),
                    new AscendingTimestamps());
            return Collections.singletonList(rowtimeAttrDescr);
        }

        @Override
        public DataStream<Row> getDataStream(StreamExecutionEnvironment streamExecutionEnvironment) {
            // create stream
            // ...
            // assign watermarks based on the "user_action_time" attribute
//            DataStream<Row> stream = inputStream.assignTimestampsAndWatermarks(...);
            DataStream<Row> stream = null;
            return stream;
        }

        @Override
        public TableSchema getTableSchema() {
            return null;
        }

    }
}
