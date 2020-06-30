package com.otis.flinksql.timeAndWatermark

import com.otis.flinksql.quickStart.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Rowtime, Schema}


object EventTimeDemo2 {
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
    tableEnv.connect(new FileSystem().path("/xxx"))
      .withFormat(new Csv)
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .rowtime(new Rowtime()
          .timestampsFromField("timestamp") //指定那个字段
          .watermarksPeriodicBounded(1000)) //设置延迟的时间
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

  }
}
