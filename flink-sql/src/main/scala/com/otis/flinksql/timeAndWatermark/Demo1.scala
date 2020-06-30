package com.otis.flinksql.timeAndWatermark

import com.otis.flinksql.表操作.StreamToView.Student
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._

/**
 * 将 DataStream转换为 Table ，并指定时间字段
 */
object Demo1 {
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

    //todo 必须是最后一个新加字段指定为proctime
    tableEnv.fromDataStream(dataStream, 'id, 'name, 'age, 'pt.proctime)


  }
}
