package org.ace.flink.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 定义表输出到控制台，一般用于开发时的调试
 * @author jace
 * @Date 2020/7/31 10:43 下午
 */
object PrintExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)
    //    源表
    tableEnv.executeSql("""CREATE TABLE source_table (
                          |  word STRING,
                          |  cnt INT
                          |) WITH (
                          |  'connector'='filesystem',
                          |  'path'='data/wordcount_out',
                          |  'format'='csv'
                          |) """.stripMargin)
    // 查询并打印
    tableEnv.executeSql("select * from  source_table").print()

    // 定义print表
    tableEnv.executeSql("""CREATE TABLE print_table (
                          | word STRING,
                          | cnt INT
                          |) WITH (
                          | 'connector' = 'print'
                          |)""".stripMargin)
    // 使用like定义print表
    tableEnv.executeSql("CREATE TABLE print_table2  WITH ('connector' = 'print') LIKE source_table (EXCLUDING ALL)")
    // 输出到目的表
    tableEnv.executeSql("insert into print_table select word,cnt from source_table")
    tableEnv.executeSql("insert into print_table2 select word,cnt from source_table")

  }
}
