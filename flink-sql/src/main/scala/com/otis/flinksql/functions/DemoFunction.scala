package com.otis.flinksql.functions

import com.otis.flinksql.quickStart.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

object DemoFunction {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream: DataStream[SensorReading] =
      env.addSource(new SensorSource)
        .assignAscendingTimestamps(_.timestamp * 1000)

    //1. 创建表环境
    val blinkSetting = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, blinkSetting)

    val table: Table = tableEnv.fromDataStream(stream)

    /**
     * table api 使用
     */
    //new 自己创建的标量函数
    val code = new HashCode

    table.select('id, code('id)).toAppendStream[Row].print("haha")


    /**
     * sql 使用方法
     */
    tableEnv.createTemporaryView("inputtable", table)
    tableEnv.registerFunction("code", code)
    tableEnv.sqlQuery(
      """
        |select id,code(id) as haha
        |from inputtable
        |""".stripMargin)
        .toAppendStream[Row].print("aaaaaaaa")

    env.execute()
  }

  //自定义一个求hashcode的标量函数
  class HashCode extends ScalarFunction {
    def eval(value: String): Int = {
      value.hashCode
    }
  }

}
