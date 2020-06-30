package com.otis.flinksql.需求

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Table}
import org.apache.flink.types.Row

object HotItems {

  case class UserBehavior(userId: Long,
                          itemId: Long,
                          categoryId: Int,
                          behavior: String,
                          timestamp: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)


    val stream = env.readTextFile("D:\\Java\\project\\bigdata\\bigDataStudy\\flink-sql\\src\\main\\resources\\UserBehavior.csv")
      .map(x => {
        val arr = x.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      }
      ).assignAscendingTimestamps(_.timestamp * 1000L)


    //1. 创建表环境
    val blinkSetting = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, blinkSetting)
    val table: Table = tableEnv.fromDataStream(stream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    //分组开窗聚合
    val aggtable = table.filter('behavior === "pv")
      .window(Slide over 1.hours every 5.minutes on 'ts as 'w)
      .groupBy('itemId, 'w)
      .select('itemId, 'itemId.count as 'cnt, 'w.end as 'wend)


        //用over窗口 row_number，求top
        tableEnv.createTemporaryView("midtable", aggtable, 'itemId, 'cnt, 'wend)
        tableEnv.sqlQuery(
          """
            |
            |select
            |*
            |from
            |(
            |select
            |*,
            |row_number() over(partition by wend order by cnt) as rn
            |from midtable
            |) where rn <=5
            |""".stripMargin)
          .toRetractStream[Row].print()

    env.execute()
  }
}
