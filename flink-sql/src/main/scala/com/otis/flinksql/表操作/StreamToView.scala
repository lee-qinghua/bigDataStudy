package com.otis.flinksql.表操作

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._

object StreamToView {

  case class Student(id: String, name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    //流的source
    val stream = env.socketTextStream("hadoop101", 7777)
    //map成样例类
    val dataStream: DataStream[Student] = stream.map(x => {
      val arr = x.split(",")
      Student(arr(0), arr(1), arr(2).toInt)
    })

    //创建表的执行环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner() //使用老版本
      .inStreamingMode() //流处理模式
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    //创建一个视图，视图的字段默认也是和样例类一样，也可以只选几个字段，或者改变字段的名字
    tableEnv.createTemporaryView("myTable", dataStream, 'id, 'name as 'sname)

    //也可以基于table创建视图
    val table1 = tableEnv.fromDataStream(dataStream)
    tableEnv.createTemporaryView("tableview", table1)

  }
}
