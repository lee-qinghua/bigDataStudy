package com.otis.flinksql.quickStart

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}

object DemoFromFile {
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
    tableEnv2.connect(new FileSystem().path("D:\\Java\\project\\bigdata\\bigDataStudy\\flink-sql\\src\\main\\resources\\test.txt"))
      .withFormat(new OldCsv()) //指定读取文件的格式,以逗号隔开
      .withSchema(new Schema().field("id", DataTypes.STRING()).field("name", DataTypes.STRING())) //类似于把读取的内容map成什么格式,就是定义表的结构
      .createTemporaryTable("studentTable") //在表环境中注册一张表

    //测试输出
    val table: Table = tableEnv2.from("studentTable")
    table.toAppendStream[(String,String)].print()

    env.execute("aaa")
  }
}
