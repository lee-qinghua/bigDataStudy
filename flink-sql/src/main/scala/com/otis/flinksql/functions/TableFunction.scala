package com.otis.flinksql.functions

import com.otis.flinksql.quickStart.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object TableFunction {
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

    //创建自定义的tableFunction
    val myTableFunction = new MyTableFunction("_")

    /**
     * table api 使用
     */

    table.joinLateral(myTableFunction('id) as('word, 'length))
      .select('id, 'word, 'length)
    //      .toAppendStream[Row].print()

    /**
     * sql 使用
     */
    tableEnv.createTemporaryView("input", table)
    tableEnv.registerFunction("split", myTableFunction)
    tableEnv.sqlQuery(
      """
        |select id,word,length
        |from input,lateral table(split(id)) as T(word,length)
        |""".stripMargin)
      .toAppendStream[Row].print()

    env.execute()
  }

  //自定义table function 实现分割字符串并统计长度
  //TableFunction[(String,Int)] 这里的类型是一个二元组，因为我想要输出的是（word,length）

  class MyTableFunction(splitstr: String) extends TableFunction[(String, Int)] {
    def eval(str: String): Unit = {
      //这里是没有返回值，类似于flatMap。而是调用collector输出
      str.split(splitstr).foreach(
        x => collect((x, x.length))
      )
    }
  }

}
