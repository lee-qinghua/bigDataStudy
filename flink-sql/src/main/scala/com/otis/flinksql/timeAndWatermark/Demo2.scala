package com.otis.flinksql.timeAndWatermark

import com.otis.flinksql.表操作.StreamToView.Student
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
 * 定义schema时指定时间字段
 */
object Demo2 {
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

    // 定义schema时指定时间
    tableEnv.connect(new FileSystem().path("/xxx"))
      .withFormat(new Csv)
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("name", DataTypes.STRING())
        .field("age", DataTypes.INT())
        .field("pt", DataTypes.TIMESTAMP(3)) //这是新加的字段，格式必须是这个
        .proctime() // 指定 pt字段为处理时间
      )
      .createTemporaryTable("inputTable")


  }
}
