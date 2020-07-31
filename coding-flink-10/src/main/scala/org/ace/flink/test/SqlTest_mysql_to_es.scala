package org.ace.flink.test

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
 * mysql 到 文件
 */
object SqlTest_mysql_to_es {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.sqlUpdate("""CREATE TABLE sourceTable (
                         |  UUID STRING,
                         |  FuncName STRING
                         |) WITH (
                         |  'connector.type' = 'jdbc', -- required: specify this table type is jdbc
                         |  'connector.url' = 'jdbc:mysql://9.134.43.59:3306/ehr_data', -- required: JDBC DB url
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
                         | 'format.type' = 'json',
                         |  'connector.type' = 'elasticsearch',
                         | 'connector.version' = '7',          -- required: valid connector versions are "6"
                         |  'connector.hosts' = 'http://localhost:9200',  -- required: one or more Elasticsearch hosts to connect to
                         |  'connector.index' = 'myuser',       -- required: Elasticsearch index
                         |  'connector.document-type' = 'user',  -- required: Elasticsearch document type
                         |  'update-mode' = 'append'
                         |)  """.stripMargin)
    tableEnv.sqlUpdate("insert into sinkTable select UUID,FuncName from sourceTable ")
    env.execute()
  }
}
