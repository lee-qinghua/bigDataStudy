package com.otis.flinksql.functions

import com.otis.flinksql.quickStart.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object TableAggregateFunction {
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

    //创建自定义的表聚合函数
    val top2Function = new Top2Function
    table.groupBy('id)
      .flatAggregate(top2Function('temperature) as('temp, 'rank))
      .select('id, 'temp, 'rank)
      .toRetractStream[Row].print()
    env.execute()
  }

  //自定义状态类

  class Top2TempAcc {
    var highestTemp: Double = Double.MinValue
    var secondHighestTemp: Double = Double.MinValue
  }

  //自定义表聚合函数实现topn 多对多
  class Top2Function extends TableAggregateFunction[(Double, Int), Top2TempAcc] {
    override def createAccumulator(): Top2TempAcc = new Top2TempAcc

    //每来一个元素的计算方法
    def accumulate(acc: Top2TempAcc, temp: Double): Unit = {
      //判断当前温度值和 两个温度对比
      if (temp > acc.highestTemp) {
        acc.secondHighestTemp = acc.highestTemp
        acc.highestTemp = temp
      } else if (temp > acc.secondHighestTemp) {
        acc.secondHighestTemp = temp
      }
    }

    //实现一个输出数据的方法，写入结果中
    def emitValue(acc: Top2TempAcc, out: Collector[(Double, Int)]) = {
      out.collect(acc.highestTemp, 1)
      out.collect(acc.secondHighestTemp, 2)
    }
  }

}
