package org.ace.flink.test

import org.ace.flink.source.AutoGenerateSource
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object TestBatch {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val data = env.fromElements(Seq(1,2,3))
    data.print()
    env.execute("testSource")
  }


}
