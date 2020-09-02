package org.ace.flink.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * kafka与mysql表关联
 * @author jace
 * @Date 2020/7/31 10:43 下午
 */
object KafkaJoinJDBC {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)
    //    源表
    tableEnv.executeSql("""CREATE TABLE kafka_table (
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
                          | 'properties.bootstrap.servers' = '10.99.218.3:9092',
                          | 'properties.group.id' = 'testGroup',
                          | 'format' = 'json', -- 'csv', 'json', 'avro', 'debezium-json' and 'canal-json'
                          | 'scan.startup.mode' = 'earliest-offset' -- 'earliest-offset', 'latest-offset', 'group-offsets', 'timestamp' and 'specific-offsets'
                          |) """.stripMargin)

    tableEnv.executeSql("""CREATE TABLE mysql_table (
                          |  type STRING,
                          |  other int
                          |) WITH (
                          |   'connector' = 'jdbc',
                          |   'url' = 'jdbc:mysql://9.134.43.59:3306/ehr_data',
                          |   'table-name' = 'ChannelJoinTest',
                          |   'username' = 'root',
                          |   'password' = 'mysql@123',
                          |   'query' = 'select type,other from ChannelJoinTest'
                          |)""".stripMargin)

    tableEnv.executeSql("""CREATE TABLE print_table (
                          | uid STRING,
                          | type STRING,
                          | other int
                          |) WITH (
                          | 'connector' = 'print'
                          |)""".stripMargin)

  tableEnv.executeSql("insert into print_table select a.uid,a.type,b.other from kafka_table a join mysql_table b on a.type=b.type").print()


  }
}
