package org.ace.flink.test

import org.ace.flink.source.AutoGenerateSource
import org.apache.flink.streaming.api.scala._

object TestSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val lines = env.addSource(new AutoGenerateSource())

    lines.print("source")
    env.execute("testSource")
  }


}
