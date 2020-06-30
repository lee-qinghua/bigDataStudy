package com.otis.flinksql.timeAndWatermark

import com.otis.flinksql.表操作.StreamToView.Student
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
 * 定义DDL时指定时间字段
 */
object Demo3 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //流的source
    //    val stream = env.socketTextStream("hadoop101", 7777)
    val stream = env.readTextFile("D:\\Java\\project\\bigdata\\bigDataStudy\\flink-sql\\src\\main\\resources\\test.txt")

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

    // DDL指定时间字段
    val sinkDDL: String =
      """
        |create table myinputtable (
        |  id varchar(20) not null,
        |  name varchar(20) not null,
        |  age bigint,
        |  pt AS PROCTIME()
        |) with (
        |  'connector.type' = 'filesystem',
        |  'connector.path' = 'file:///D:\\..\\sensor.txt',
        |  'format.type' = 'csv'
        |)
  """.stripMargin
    //todo 必须使用Blink planner，我这里使用的老版本，需要更改
    tableEnv.sqlUpdate(sinkDDL)
  }
}
