package org.ace.flink.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 实时读取Mysql数据，需要mysql 配置binlog_row_image=FULL
 * 提交到集群或sql-client中可以正常运行
 * @author jace
 * @Date 2020/9/2 9:53 上午
 */
object MysqlCDC {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)
    //    源表
    tableEnv.executeSql("""CREATE TABLE source_table (
                          |  UUID STRING,
                          |  AppKey STRING
                          |) WITH (
                          |    'connector' = 'mysql-cdc',
                          |    'hostname' = 'localhost',
                          |    'port' = '3306',
                          |    'username' = 'root',
                          |    'password' = 'mysql@123',
                          |    'database-name' = 'ehr_data',
                          |    'table-name' = 'AppInfo'
                          |)""".stripMargin)


    tableEnv.executeSql("""CREATE TABLE target_table (
                          |  UUID STRING,
                          |  AppKey STRING,
                          |  PRIMARY KEY (UUID) NOT ENFORCED
                          |) WITH (
                          |   'connector' = 'jdbc',
                          |   'url' = 'jdbc:mysql://9.134.43.59:3306/ehr_data',
                          |   'table-name' = 'ChannelTest',
                          |   'username' = 'root',
                          |   'password' = 'mysql@123'
                          |)""".stripMargin)
// sql-client可以直接select打印
//    tableEnv.executeSql("select * from source_table").print()
    // 使用like定义print表
    tableEnv.executeSql("CREATE TABLE print_table  WITH ('connector' = 'print') LIKE source_table (EXCLUDING ALL)")
    //输出到目的表
    tableEnv.executeSql("insert into print_table select UUID,AppKey from source_table")
    // 能增删查改
    tableEnv.executeSql("insert into target_table select UUID,AppKey from source_table group by UUID,AppKey")

  }
}
