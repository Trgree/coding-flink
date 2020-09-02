package org.ace.flink.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 实时读取Mysql数据，需要mysql 配置binlog_row_image=FULL
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
                          |    'hostname' = '9.134.43.59',
                          |    'port' = '3306',
                          |    'username' = 'root',
                          |    'password' = 'mysql@123',
                          |    'database-name' = 'ehr_data',
                          |    'table-name' = 'AppInfo'
                          |)""".stripMargin)

//    tableEnv.executeSql("select * from source_table").print()
    // 使用like定义print表
    tableEnv.executeSql("CREATE TABLE print_table  WITH ('connector' = 'print') LIKE source_table (EXCLUDING ALL)")
    // 输出到目的表
    tableEnv.executeSql("insert into print_table select UUID,AppKey from source_table")

  }
}
