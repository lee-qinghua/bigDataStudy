package com.otis.flinksql.timeAndWatermark

import com.otis.flinksql.quickStart.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Rowtime, Schema}


object EventTimeDemo3 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    //流的source
    //    val stream = env.socketTextStream("hadoop101", 7777)
    val stream = env.readTextFile("D:\\Java\\project\\bigdata\\bigDataStudy\\flink-sql\\src\\main\\resources\\test.txt")

    //map成样例类
    val dataStream: DataStream[SensorReading] = stream.map(x => {
      val arr = x.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
      })

    //创建表的执行环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner() //使用老版本
      .inStreamingMode() //流处理模式
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 定义schema时指定时间
    val sinkDDL: String =
      """
        |create table dataTable (
        |  id varchar(20) not null,
        |  ts bigint,
        |  temperature double,
        |  rt AS TO_TIMESTAMP( FROM_UNIXTIME(ts) ),
        |  watermark for rt as rt - interval '1' second
        |) with (
        |  'connector.type' = 'filesystem',
        |  'connector.path' = 'file:///D:\\..\\sensor.txt',
        |  'format.type' = 'csv'
        |)
""".stripMargin
    tableEnv.sqlUpdate(sinkDDL) // 执行 DDL

  }
}
