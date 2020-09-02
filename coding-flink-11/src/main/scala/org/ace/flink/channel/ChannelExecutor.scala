package org.ace.flink.channel

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.{SerializeConfig, SerializerFeature}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 管道语句执行器
 *
 * @author jace
 * @Date 2020/9/1 5:20 下午
 */
object ChannelExecutor {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    val tables = params.getRequired("tables")
    val sqls = params.getRequired("sqls")

    // 建表
    for (table <- tables.split(";")) {
      println(table)
      tableEnv.executeSql(table)
    }
    // 执行insert into
    for (sql <- sqls.split(";")) {
      println(sql)
      tableEnv.executeSql(sql)
    }

  }

}
