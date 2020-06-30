package com.otis.flinksql.functions

import com.otis.flinksql.quickStart.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

object AggregateFunctionTest {
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

    //创建自定义函数的实例
    val avgTemp = new AvgTemp

    //todo 聚合函数可以不开窗使用，但是一般都和window配合使用。
    // 不开窗的话会一致更新结果。
    /**
     * table api 使用
     */
    table.groupBy('id)
      .aggregate(avgTemp('temperature) as 'pingjun)
      .select('id, 'pingjun)
      .toRetractStream[Row].print()
    /**
     * sql 使用
     * 1. 创建表视图
     * 2. 注册函数
     * 3. select id,MyAggFunction(temp) as avgTemp from inputtable group by id
     */

    env.execute()
  }

  //定义一个中间结果的样例类，用于保存聚合状态
  class AvgAcc {
    var sum: Double = 0.0
    var count: Int = 0
  }

  //定义一个聚合函数
  class AvgTemp extends AggregateFunction[Double, AvgAcc] {
    override def getValue(acc: AvgAcc): Double = acc.sum / acc.count

    override def createAccumulator(): AvgAcc = new AvgAcc

    //每来一个元素的计算方法
    def accumulate(acc: AvgAcc, temp: Double): Unit = {
      acc.sum += temp
      acc.count += 1
    }
  }

}
