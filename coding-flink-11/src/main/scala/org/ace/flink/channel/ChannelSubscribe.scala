package org.ace.flink.channel

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 订阅
 *
 * @author jace
 * @Date 2020/9/1 11:24 上午
 */
object ChannelSubscribe {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)

    // 定义mysql源表
    tableEnv.executeSql("""CREATE TABLE source_table (
                          |  UUID STRING,
                          |  AppKey STRING
                          |) WITH (
                          |   'connector' = 'jdbc',
                          |   'url' = 'jdbc:mysql://dev-mysql:3306/ehr_data',
                          |   'table-name' = 'AppInfo', --可不用
                          |   'username' = 'root',
                          |   'password' = 'mysql@123',
                          |   'query' = 'select a.UUID,a.AppKey from AppInfo a join CorpAppInfo b ON a.AppKey=b.AppKey'
                          |)""".stripMargin)
    // 定义hdfs输出
    tableEnv.executeSql("""CREATE TABLE hdfs_table (
                          |  UUID STRING,
                          |  AppKey STRING
                          |) WITH (
                          |  'connector'='filesystem',
                          |  'path'='/Users/jace/tmp/channel/sink_table/',
                          |  'format'='csv'
                          |) """.stripMargin)

    // 输出到hdfs
    tableEnv.executeSql("insert into hdfs_table select UUID,AppKey from source_table")

  }

}
