package org.ace.flink.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 自动生成测试数据
 *
 * @author jace
 * @Date 2020/7/31 10:43 下午
 */
object GenearteToPrint {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)
    // 自动生成数据的源表
    tableEnv.executeSql("""CREATE TABLE source_table (
                          | f_sequence INT,
                          | f_random INT,
                          | f_random_str STRING,
                          | ts AS localtimestamp,
                          | WATERMARK FOR ts AS ts
                          |) WITH (
                          | 'connector' = 'datagen',
                          |
                          | -- optional options --
                          | 'rows-per-second'='5',
                          | 'fields.f_sequence.kind'='sequence',
                          | 'fields.f_sequence.start'='1',
                          | 'fields.f_sequence.end'='1000',
                          |
                          | 'fields.f_random.min'='1',
                          | 'fields.f_random.max'='1000',
                          |
                          | 'fields.f_random_str.length'='10'
                          |) """.stripMargin)


    // 使用like定义print表
    tableEnv.executeSql("CREATE TABLE print_table  WITH ('connector' = 'print') LIKE source_table (EXCLUDING ALL)")
    // 输出到目的表
    tableEnv.executeSql("insert into print_table select f_sequence,f_random,f_random_str from source_table")

  }
}
