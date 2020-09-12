package org.ace.flink.test

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
 * mysql 到 文件
 * flink-jdbk_2.11:2.10.0不支持sql server,需要修改JDBCDialects,增加SqlServerDialect
 */
object SqlTest_sqlserver_to_file {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.sqlUpdate("""CREATE TABLE sourceTable (
                         |  OrganizationWID BIGINT,
                         |  OrganizationName STRING
                         |) WITH (
                         |  'connector.type' = 'jdbc', -- required: specify this table type is jdbc
                         |  'connector.url' = 'jdbc:sqlserver://dev-mysql;database=DM_A', -- required: JDBC DB url
                         |  'connector.table' = 'DM.DM_DIM_Organization',  -- required: jdbc table name
                         |  'connector.driver' = 'com.microsoft.sqlserver.jdbc.SQLServerDriver', -- optional: the class name of the JDBC driver to use to connect to this URL.
                         |                                                -- If not set, it will automatically be derived from the URL.
                         |  'connector.username' = 'c_tbds', -- optional: jdbc user name and password
                         |  'connector.password' = '7165189C8F75410FA1FCA37E57FD521F'
                         |)  """.stripMargin)

    tableEnv.sqlUpdate("""CREATE TABLE sinkTable (
                         |  OrganizationWID BIGINT,
                         |  OrganizationName STRING
                         |) WITH (
                         |  'connector.type' = 'filesystem',
                         |  'connector.path' = 'data/sink_sqlserver.csv',
                         |  'format.type' = 'csv'
                         |)  """.stripMargin)
    tableEnv.sqlUpdate("insert into sinkTable select OrganizationWID,OrganizationName  from sourceTable WHERE OrganizationWID > 4351644 and OrganizationWID < 4354641")
    env.execute()
  }
}
