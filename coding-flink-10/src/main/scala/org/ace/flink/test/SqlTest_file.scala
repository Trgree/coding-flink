package org.ace.flink.test

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
 * 文件 到 文件
 */
object SqlTest_file {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.sqlUpdate("""CREATE TABLE sourceTable (
                         |  name STRING,
                         |  age BIGINT
                         |) WITH (
                         |  'connector.type' = 'filesystem',
                         |  'connector.path' = 'data/test.csv',
                         |  'format.type' = 'csv'
                         |)  """.stripMargin)
    tableEnv.sqlUpdate("""CREATE TABLE sinkTable (
                         |  name STRING,
                         |  age BIGINT
                         |) WITH (
                         |  'connector.type' = 'filesystem',
                         |  'connector.path' = 'data/sink.csv',
                         |  'format.type' = 'csv'
                         |)  """.stripMargin)
    tableEnv.sqlUpdate("insert into sinkTable select * from sourceTable")
    env.execute()
  }
}

case class User(name :String,age :Long)