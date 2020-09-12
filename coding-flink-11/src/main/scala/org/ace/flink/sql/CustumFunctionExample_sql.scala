package org.ace.flink.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.ScalarFunction

/**
 * 自定义函数(create function方式)
 *
 * @author jace
 * @Date 2020/7/31 10:43 下午
 */
object CustumFunctionExample_sql {
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
    // 注册函数
    val funcionSql = "create TEMPORARY function IF NOT EXISTS subStrs AS 'org.ace.flink.sql.SubstringFunction'"
    tableEnv.executeSql(funcionSql)
    // 使用like定义print表
    tableEnv.executeSql("CREATE TABLE print_table  WITH ('connector' = 'print') LIKE source_table (EXCLUDING ALL)")
    // 输出到目的表
    tableEnv.executeSql("insert into print_table select f_sequence,f_random,subStrs(f_random_str,0,1) from source_table")

  }

  /**
   * 根据className获取对象class
   * @param className
   * @return
   */
  def getClazz(className: String): Class[ScalarFunction] ={
    Class.forName(className).asInstanceOf[Class[ScalarFunction]]
  }

}


