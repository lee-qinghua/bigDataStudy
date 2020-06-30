package com.otis.flinksql.functions

import org.apache.flink.table.functions.ScalarFunction

object DemoFunction {
  def main(args: Array[String]): Unit = {


    
  }

  //自定义一个求hashcode的标量函数
  class HashCode extends ScalarFunction {
    def eval(value: String): Int = {
      value.hashCode
    }
  }

}
