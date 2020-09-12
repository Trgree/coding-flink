package org.ace.flink.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * kafka读取(待测试)
 * @author jace
 * @Date 2020/7/31 10:43 下午
 */
object KafkaToPrint {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)
    //    源表
    tableEnv.executeSql("""CREATE TABLE source_table (
                          |  uid STRING,
                          |  type STRING,
                          |  staffInfo ROW<
                          |   staffId INT
                          |  >,
                          |  clientInfo ROW<
                          |   location ROW<
                          |     lat FLOAT
                          |   >
                          |  >
                          |) WITH (
                          | 'connector' = 'kafka-0.11',
                          | 'topic' = 'visitLog_tdw',
                          | 'properties.bootstrap.servers' = 'localhost:9092',
                          | 'properties.group.id' = 'testGroup',
                          | 'format' = 'json', -- 'csv', 'json', 'avro', 'debezium-json' and 'canal-json'
                          | 'scan.startup.mode' = 'earliest-offset' -- 'earliest-offset', 'latest-offset', 'group-offsets', 'timestamp' and 'specific-offsets'
                          |) """.stripMargin)

    // 使用like定义print表
    tableEnv.executeSql("CREATE TABLE print_table  WITH ('connector' = 'print') LIKE source_table (EXCLUDING ALL)")
    // 输出到目的表
    tableEnv.executeSql("insert into print_table select * from source_table")

  }
}
