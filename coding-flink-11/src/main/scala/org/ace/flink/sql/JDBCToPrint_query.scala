package org.ace.flink.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * JDBC读取（待测试）
 *
 * @author jace
 * @Date 2020/7/31 10:43 下午
 */
object JDBCToPrint_query {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)
    //    源表
    tableEnv.executeSql("""CREATE TABLE source_table (
                          |  id BIGINT,
                          |  name STRING,
                          |  age INT,
                          |  status BOOLEAN,
                          |  PRIMARY KEY (id) NOT ENFORCED
                          |) WITH (
                          |   'connector' = 'jdbc',
                          |   'url' = 'jdbc:mysql://localhost:3306/mydatabase',
                          |   'table-name' = 'users',
                          |   'username' = '',
                          |   'password' = '',
                          |   'connector.read.query' = 'select * from users'
                          |)""".stripMargin)

    // 使用like定义print表
    tableEnv.executeSql("CREATE TABLE print_table  WITH ('connector' = 'print') LIKE source_table (EXCLUDING ALL)")
    // 输出到目的表
    tableEnv.executeSql("insert into print_table select id,name,age,status from source_table")

  }
}
