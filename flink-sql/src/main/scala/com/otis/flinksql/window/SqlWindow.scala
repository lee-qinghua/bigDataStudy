package com.otis.flinksql.window

import com.otis.flinksql.quickStart.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object SqlWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

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

    tableEnv.createTemporaryView("inputTable", dataStream, 'id, 'timestamp.rowtime as 'rt, 'temperature)

    tableEnv.sqlQuery(
      """
        |select id,count(id) ct,
        |hop_end(rt,interval '5' second,interval '10' second)
        |from inputTable
        |group by id, hop(rt,interval '5' second,interval '10' second)
        |""".stripMargin)

    tableEnv.sqlQuery(
      """
        |select id,count(id) ct,
        |tumble_end(rt,interval '5' minute)
        |from inputTable
        |group by id, tumble(rt,interval '5' minute)
        |""".stripMargin)
    //      .toAppendStream[Row].print()


    //over window
    tableEnv.sqlQuery(
      """
        |select id,count(id) over w,avg(temperature) over w
        |from inputTable
        |window w as(partition by id order by rt rows between 2 preceding and current row)
        |""".stripMargin)
        .toAppendStream[Row].print("haha")

    env.execute()
  }
}
