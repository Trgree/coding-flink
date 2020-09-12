package org.ace.flink.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * JDBC读取（待测试）
 *
 * @author jace
 * @Date 2020/7/31 10:43 下午
 */
object JDBCToPrint {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)
    //    源表
    tableEnv.executeSql("""CREATE TABLE source_table (
                          |  UUID STRING,
                          |  AppKey STRING
                          |) WITH (
                          |   'connector' = 'jdbc',
                          |   'url' = 'jdbc:mysql://dev-mysql:3306/ehr_data',
                          |   'table-name' = 'AppInfo', --视图也可以
                          |   'username' = 'root',
                          |   'password' = 'mysql@123'
                          |)""".stripMargin)

    // 使用like定义print表
    tableEnv.executeSql("CREATE TABLE print_table  WITH ('connector' = 'print') LIKE source_table (EXCLUDING ALL)")
    // 输出到目的表
    tableEnv.executeSql("insert into print_table select UUID,AppKey from source_table")

  }
}
