package com.otis.flinksql.window

import com.otis.flinksql.quickStart.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, GroupWindow, Slide, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object GroupWindowDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    //流的source
    //    val stream = env.socketTextStream("hadoop101", 7777)
    //      val stream = env.readTextFile("D:\\Java\\project\\bigdata\\bigDataStudy\\flink-sql\\src\\main\\resources\\test.txt")
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
    //      //2. 添加新字段
    //      tableEnv.fromDataStream(dataStream, 'id, 'timestamp, 'temperature, 'rt.rowtime)


    val table1 = table.window(Tumble over 10.minutes on 'rt as 'w)
      .groupBy('w, 'id)
      .select('id, 'id.count as 'ct, 'w.end)

    //      table1.toRetractStream[Row].print()
    table1.toAppendStream[Row].print()  //因为窗口关闭时才会输出一次结果，不会来一条做聚合一次，所以可以直接用appendStream


    //      //开窗
    //      //开滚动的事件时间窗口
    //      table.window(Tumble over 10.minutes on 'rowtime as 'w)
    //      //开滚动的处理时间窗口
    //      table.window(Tumble over 10.minutes on 'proctime as 'w)
    //      //开滚动的计数窗口
    //      table.window(Tumble over 10.rows on 'proctime as 'w)
    //
    //      //开滑动的事件时间窗口
    //      table.window(Slide over 10.minutes every 5.minutes on 'rowtime as 'w)
    //      table.window(Slide over 10.minutes every 5.minutes on 'proctime as 'w)
    //      table.window(Slide over 10.rows every 5.rows on 'proctime as 'w)

    env.execute()
  }
}
