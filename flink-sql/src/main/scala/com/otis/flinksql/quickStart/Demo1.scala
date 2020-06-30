package com.otis.flinksql.quickStart

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

object Demo1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    //1. 创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //2. 基于tableEnv,将流转换成表
    val table: Table = tableEnv.fromDataStream(stream)

    /**
     * 3.1 这样就可以调用Table（java的接口）接口的方法，table api转换操作
     */

    val tableResult1: Table = table.select("id,temperature")
      .filter("id == 'sensor_1'")

    /**
     * 3.2 直接写sql实现
     */
    //首先注册一张表
    val myTable = tableEnv.registerTable("tableName", table)
    //sql他复杂太长，可以用三引号来自动换行
    val tableResult2: Table = tableEnv.sqlQuery(
      """
        |select id,temperature
        |from tableName
        |where id='sensor_1'
        |""".stripMargin)

    val unit: DataStream[(String, Double)] = tableResult2.toAppendStream[(String, Double)]
    unit.print()

    env.execute("sql demo")
  }
}
