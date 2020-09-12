package org.ace.flink.test

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
 * mysql 到 文件
 */
object SqlTest_mysql_to_file {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.sqlUpdate("""CREATE TABLE sourceTable (
                         |  UUID STRING,
                         |  FuncName STRING
                         |) WITH (
                         |  'connector.type' = 'jdbc', -- required: specify this table type is jdbc
                         |  'connector.url' = 'jdbc:mysql://dev-mysql:3306/ehr_data', -- required: JDBC DB url
                         |  'connector.table' = 'AppFuncInfo',  -- required: jdbc table name
                         |  'connector.driver' = 'com.mysql.jdbc.Driver', -- optional: the class name of the JDBC driver to use to connect to this URL.
                         |                                                -- If not set, it will automatically be derived from the URL.
                         |  'connector.username' = 'root', -- optional: jdbc user name and password
                         |  'connector.password' = 'mysql@123'
                         |)  """.stripMargin)

    tableEnv.sqlUpdate("""CREATE TABLE sinkTable (
                         |  UUID STRING,
                         |  FuncName STRING
                         |) WITH (
                         |  'connector.type' = 'filesystem',
                         |  'connector.path' = 'data/sink_sql.csv',
                         |  'format.type' = 'csv'
                         |)  """.stripMargin)
    tableEnv.sqlUpdate("insert into sinkTable select UUID,FuncName from sourceTable where UUID='gfh'")
    env.execute()
  }
}
