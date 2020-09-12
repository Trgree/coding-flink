package org.ace.flink.channel

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.{SerializeConfig, SerializerFeature}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 管道语句执行器
 * 输入参数
 * --tables 建表语句
 * --functions 创建function语句（可选）
 * --sqls insert into语句

命令行参数样例：
--tables "CREATE TABLE source_table (
 f_sequence INT,
 f_random INT,
 f_random_str STRING,
 ts AS localtimestamp,
 WATERMARK FOR ts AS ts
) WITH (
 'connector' = 'datagen',
 'rows-per-second'='5',
 'fields.f_sequence.kind'='sequence',
 'fields.f_sequence.start'='1',
 'fields.f_sequence.end'='1000',
 'fields.f_random.min'='1',
 'fields.f_random.max'='1000',

 'fields.f_random_str.length'='10'
)
@@@
CREATE TABLE print_table  WITH ('connector' = 'print') LIKE source_table (EXCLUDING ALL)
"
--functions "create TEMPORARY function IF NOT EXISTS subStrs AS 'org.ace.flink.sql.SubstringFunction'"
--sqls "insert into print_table select f_sequence,f_random,subStrs(f_random_str,0,1) from source_table"
-p 1
-d
-c  org.ace.flink.channel.ChannelExecutor

 * @author jace
 * @Date 2020/9/1 5:20 下午
 */
object ChannelExecutor {
  var tableEnv: StreamTableEnvironment = null
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
     tableEnv = StreamTableEnvironment.create(env)
    val tables = params.getRequired("tables")
    val sqls = params.getRequired("sqls")
    val funs = params.get("functions")


    // 建表
    executeSqls(tables)
    // 建function
    executeSqls(funs)
    // 执行insert into
    executeSqls(sqls)

  }

  def executeSqls(sqls:String): Unit ={
    if(sqls != null) {
      for (sql <- sqls.split("@@@")) {
        if(sql != null && !sql.trim().equals("")){
          println(sql)
          tableEnv.executeSql(sql)
        }
      }
    }
  }


}
