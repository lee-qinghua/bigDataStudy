package com.otis.flinksql.sink

import com.otis.flinksql.表操作.StreamToView.Student
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._

object JdbcSink {
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
    val aggResultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select id,count(id) ct
        |from myTable
        |group by id
        |""".stripMargin)

    // 写入mysql jdbc没有直接的connector，但是可以直接写update语句来进行插入
    val sinkDDL: String =
      """
        |create table jdbcOutputTable (
        |  id varchar(20) not null,
        |  ct bigint not null
        |) with (
        |  'connector.type' = 'jdbc',
        |  'connector.url' = 'jdbc:mysql://localhost:3306/test',
        |  'connector.table' = 'flink',
        |  'connector.driver' = 'com.mysql.jdbc.Driver',
        |  'connector.username' = 'root',
        |  'connector.password' = 'root'
        |)
  """.stripMargin
    tableEnv.sqlUpdate(sinkDDL)
    aggResultSqlTable.insertInto("jdbcOutputTable")

    env.execute("jdbc")
  }
}
