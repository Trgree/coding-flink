package org.ace.flink.test

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * nc -l -p 9999
  */
object WordCount {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val lines = env.socketTextStream("localhost", 9999)
    val result = lines.flatMap(l => l.split(" "))
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(4))
      .sum(1)
    result.print()
    env.execute("wordCount")
  }

}
