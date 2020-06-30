package com.otis.flinksql.sink

import com.otis.flinksql.表操作.StreamToView.Student
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

object TableSinkToKafka {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
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

    //创建视图
    tableEnv.createTemporaryView("myTable", dataStream, 'id, 'name as 'sname, 'age)

    //表的转换操作
    val table: Table = tableEnv.sqlQuery(
      """
        |select id,sname,age
        |from myTable
        |where age>18
        |""".stripMargin)


    //todo 将结果表输出到kafka
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("myTopic")
      .property("zookeeper.connect", "hadoop101:2181")
      .property("bootstrap.servers", "hadoop101:9092"))
      .withFormat(new Csv)
      .withSchema(new Schema().field("id", DataTypes.STRING()).field("sname", DataTypes.STRING()).field("age", DataTypes.INT()))
      .createTemporaryTable("kafkaOutput")

    table.insertInto("kafkaOutput")
    env.execute("output")
  }
}
