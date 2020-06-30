package com.otis.flinksql.window

import com.otis.flinksql.quickStart.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Over}
import org.apache.flink.types.Row

object OverWindowDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    //流的source
    //    val stream = env.socketTextStream("hadoop101", 7777)


    val dataStream = env.addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
      })

    //创建表的执行环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner() //使用老版本
      .inStreamingMode() //流处理模式
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    //1. 直接指定字段
    val table = tableEnv.fromDataStream(dataStream, 'id, 'timestamp.rowtime as 'rt, 'temperature)

    table.window(Over partitionBy 'id orderBy 'rt preceding 2.minutes as 'w)
      .select('id, 'rt, 'id.count over 'w, 'temperature.avg over 'w)
      .toAppendStream[Row].print()
    //      // 无界的事件时间
    //      table.window(Over partitionBy 'a orderBy 'rowtime preceding UNBOUNDED_RANGE as 'w)
    //      // 无界的处理时间
    //      //over window
    //      table.window(Over partitionBy 'a orderBy 'proctime preceding UNBOUNDED_RANGE as 'w)
    //      // 无界的事件时间
    //      //Row-count over window
    //      table.window(Over partitionBy 'a orderBy 'rowtime preceding UNBOUNDED_ROW as 'w)
    //      // 无界的处理时间
    //      //Row-count over window
    //      table.window(Over partitionBy 'a orderBy 'proctime preceding UNBOUNDED_ROW as 'w)
    //
    //
    //
    //      // 有界的事件时间
    //      //over window
    //      table.window(Over partitionBy 'a orderBy 'rowtime preceding 1.minutes as 'w)
    //      // 有界的处理时间
    //      //over window
    //      table.window(Over partitionBy 'a orderBy 'proctime preceding 1.minutes as 'w)
    //      // 有界的事件时间
    //      //Row-count over window
    //      table.window(Over partitionBy 'a orderBy 'rowtime preceding 10.rows as 'w)
    //      // 有界的处理时间
    //      // Row-count over window
    //      table.window(Over partitionBy 'a orderBy 'proctime preceding 10.rows as 'w)
    env.execute()

  }
}
