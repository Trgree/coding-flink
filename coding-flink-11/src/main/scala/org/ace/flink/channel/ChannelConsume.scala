package org.ace.flink.channel

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 消费
 *
 * @author jace
 * @Date 2020/9/1 11:24 上午
 */
object ChannelConsume {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)

    // 定义hdfs源
    tableEnv.executeSql("""CREATE TABLE hdfs_table (
                          |  UUID STRING,
                          |  AppKey STRING
                          |) WITH (
                          |  'connector'='filesystem',
                          |  'path'='/Users/jace/tmp/channel/sink_table',
                          |  'format'='csv'
                          |) """.stripMargin)

    // 定义mysql输出
    tableEnv.executeSql("""CREATE TABLE sink_table_mysql (
                          |  UUID STRING,
                          |  AppKey STRING,
                          |  other INT
                          |) WITH (
                          |   'connector' = 'jdbc',
                          |   'url' = 'jdbc:mysql://dev-mysql:3306/ehr_data',
                          |   'table-name' = 'ChannelTest', --可不用
                          |   'username' = 'root',
                          |   'password' = 'mysql@123'
                          |)""".stripMargin)

    // 过滤、加工
    val sql = "select UUID,UPPER(AppKey),1 from hdfs_table"
    // 输出到hdfs
    tableEnv.executeSql("insert into sink_table_mysql " + sql)

  }

}
