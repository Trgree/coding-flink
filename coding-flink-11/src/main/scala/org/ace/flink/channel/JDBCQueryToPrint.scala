package org.ace.flink.channel

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * JDBC读取
 * 根据query语句读取源表，需要修改flink 1.11源码，这里使用 1.11版本
 *
 * @author jace
 * @Date 2020/7/31 10:43 下午
 */
object JDBCQueryToPrint {
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
                          |   'url' = 'jdbc:mysql://9.134.43.59:3306/ehr_data',
                          |   'table-name' = 'AppInfo',
                          |   'username' = 'root',
                          |   'password' = 'mysql@123',
                          |   'query' = 'select a.UUID,a.AppKey from AppInfo a join CorpAppInfo b ON a.AppKey=b.AppKey'
                          |)""".stripMargin)

    // 使用like定义print表
    tableEnv.executeSql("CREATE TABLE print_table  WITH ('connector' = 'print') LIKE source_table (EXCLUDING ALL)")
    // 输出到目的表
    tableEnv.executeSql("insert into print_table select UUID,AppKey from source_table")

  }
}
