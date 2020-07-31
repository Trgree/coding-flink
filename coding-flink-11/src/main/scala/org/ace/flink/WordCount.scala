package org.ace.flink

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._

/**
 * 入门例子
 * @author jace
 * @Date 2020/7/31 10:30 下午
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val source = env.readTextFile("data/wordcount.txt","UTF-8")
    val result = source.flatMap(_.split(" "))
      .filter(_.trim.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    result.print()

    result.writeAsCsv("data/wordcount_out",writeMode=FileSystem.WriteMode.OVERWRITE)
    env.execute("wordCount")
  }
}
