package org.ace.flink.test

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
/**
 *
 * @author jace
 * @Date 2020/8/6 11:07 下午
 */
object Test {

  def main(args: Array[String]): Unit = {
    import org.apache.flink.streaming.api.functions.source.SourceFunction
    import java.util.concurrent.TimeUnit
    import scala.util.Random

    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    val lines = senv.addSource(new SourceFunction[(String,Int)](){
      var running = true
      val machines = List("machine1", "machine2", "machine3")
      val rand = new Random()

      override def cancel(): Unit = running = false

      override def run(ctx: SourceFunction.SourceContext[(String,Int)]): Unit = {
        while(running){
          val machine = machines(rand.nextInt(machines.size))
          ctx.collect((machine, rand.nextInt(100)))
          TimeUnit.SECONDS.sleep(1)
        }
      }
    })
    lines.print("source")

    senv.execute()
  }
}
