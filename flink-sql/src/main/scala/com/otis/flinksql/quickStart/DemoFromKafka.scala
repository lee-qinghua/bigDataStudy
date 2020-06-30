package com.otis.flinksql.quickStart

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, OldCsv, Schema}

object DemoFromKafka {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)


    /**
     * 老版本planner的流处理设置
     */
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner() //使用老版本
      .inStreamingMode() //流处理模式
      .build()
    val tableEnv2 = StreamTableEnvironment.create(env, settings)

    /**
     * 老版本planner的批处理设置
     */
    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
    val batchTableEnv = BatchTableEnvironment.create(batchEnv)

    /**
     * 新版本 Blink 基于Blink 新版本是批流统一，只需要更改 inxxxMode即可
     */
    //Blink  流处理
    val blinkSetting = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val bsTableEnv = StreamTableEnvironment.create(env, blinkSetting)

    //Blink  批处理
    val bbSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode().build()
    val bbTableEnv = TableEnvironment.create(bbSettings)


    //2. 读取数据


    tableEnv2.connect(new Kafka()
      .version("0.11")
      .topic("myTopic")
      .property("zookeeper.connect", "")
      .property("bootstrap.servers", ""))
      .withFormat(new Csv()) //目前支持 csv json
      .withSchema(new Schema().field("id", DataTypes.STRING()).field("name", DataTypes.STRING()))
      .createTemporaryTable("kafkaTable")


    //3. 测试输出
    //从环境中获取注册的表，就可以获取表对象，然后就可以调用api
    val table: Table = tableEnv2.from("kafkaTable")
    val table1: Table = table
      .select("id,name")
      //  .select('id,'name) 或者这样写，scala的格式
      .filter('id === "1")

    //分id 聚合求次数
    val table2: Table = table1.groupBy('id)
      .select('id, 'id.count as 'mycount)
    table2.toRetractStream[(String, Int)].print()

    //    table2.toAppendStream[(String, String)].print()

    tableEnv2.sqlQuery(
      """
        |select id ,count(id) as mycount
        |from kafkaTable
        |group by id
        |""".stripMargin)


    env.execute("aaa")
  }
}
