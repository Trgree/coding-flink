package org.ace.flink.test

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
 * mysql 到 文件
 */
object SqlTest_es_to_file {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.sqlUpdate("""CREATE TABLE sourceTable (
                         |  UUID STRING,
                         |  FuncName STRING
                         |) WITH (
                         | 'format.type' = 'json',
                         |  'connector.type' = 'elasticsearch',
                         | 'connector.version' = '7',          -- required: valid connector versions are "6"
                         |  'connector.hosts' = 'http://localhost:9200',  -- required: one or more Elasticsearch hosts to connect to
                         |  'connector.index' = 'myuser',       -- required: Elasticsearch index
                         |  'connector.document-type' = 'user',  -- required: Elasticsearch document type
                         |)  """.stripMargin)

    tableEnv.sqlUpdate("""CREATE TABLE sinkTable (
                         |  UUID STRING,
                         |  FuncName STRING
                         |) WITH (
                         |  'connector.type' = 'filesystem',
                         |  'connector.path' = 'data/sink_es.csv',
                         |  'format.type' = 'csv'
                         |)  """.stripMargin)
    tableEnv.sqlUpdate("insert into sinkTable select UUID,FuncName from sourceTable ")
    env.execute()
  }
}
